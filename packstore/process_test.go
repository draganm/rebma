package packstore

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/draganm/rebma/key"
)

const childDirEnv = "PACKSTORE_TEST_CHILD_DIR"

var childData = []byte("written by the child process")

// TestMain doubles as the second process of the tests below: with
// childDirEnv set, the test binary opens that store, writes one object and
// exits.
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
	s, err := Open(dir, WithSync(false))
	if err != nil {
		return err
	}
	k, err := key.New(key.Blob, uint64(len(childData)), childData)
	if err != nil {
		return err
	}
	if err := s.Put(k, childData); err != nil {
		return err
	}
	return s.Close()
}

func TestSecondProcessWritesAreVisible(t *testing.T) {
	dir := t.TempDir()
	s := openStore(t, dir, WithSync(false)) // open, and owning a segment, for the child's whole life
	putAll(t, s, testObjects(t, 1))

	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), childDirEnv+"="+dir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child process: %v\n%s", err, out)
	}
	k, err := key.New(key.Blob, uint64(len(childData)), childData)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(k)
	if err != nil || !bytes.Equal(got, childData) {
		t.Fatalf("Get(the child's object) = %q, %v", got, err)
	}
	if n := len(activeFiles(t, dir)); n != 2 {
		t.Fatalf("%d active segments, want the parent's and the child's", n)
	}
}
