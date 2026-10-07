package gc

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/draganm/rebma/packstore"
)

const childDirEnv = "GC_TEST_CHILD_DIR"

// TestMain doubles as the second process of TestCycleWaitsForAnotherProcess:
// with childDirEnv set, the test binary opens that store, enters a write
// span, says so, and stays in it until its stdin closes.
func TestMain(m *testing.M) {
	if dir := os.Getenv(childDirEnv); dir != "" {
		if err := childProcess(dir); err != nil {
			fmt.Fprintln(os.Stderr, "child:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func childProcess(dir string) error {
	objects, err := packstore.Open(filepath.Join(dir, "packstore"))
	if err != nil {
		return err
	}
	endSpan, err := objects.BeginWrite()
	if err != nil {
		return err
	}
	fmt.Println("in a write span")
	io.Copy(io.Discard, os.Stdin) // until the parent has seen enough
	endSpan()
	return objects.Close()
}

func TestCycleWaitsForAnotherProcess(t *testing.T) {
	ts := newTestStore(t, 4<<10)
	c := ts.openCollector(t, Options{Grace: time.Hour})

	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), childDirEnv+"="+ts.dir)
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Wait()
	defer stdin.Close()
	if line, err := bufio.NewReader(stdout).ReadString('\n'); err != nil {
		t.Fatalf("the child never entered its write span: %q, %v", line, err)
	}

	var runErr error
	ran := async(func() { _, runErr = c.Run(context.Background(), 0) })
	stillWaiting(t, ran, "a cycle, while another process is in a write span,")
	stdin.Close()
	finishes(t, ran, "the cycle, after the other process left its write span,")
	if runErr != nil {
		t.Fatal(runErr)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("child process: %v", err)
	}
}
