{
  description = "github.com/draganm/rebma";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

    systems.url = "github:nix-systems/default";

  };

  outputs = { self, nixpkgs, systems, ... }@inputs:
    let
      eachSystem = f:
        nixpkgs.lib.genAttrs (import systems)
        (system: f system nixpkgs.legacyPackages.${system});
    in {
      formatter = eachSystem (system: pkgs:
        pkgs.writeShellApplication {
          name = "format";
          runtimeInputs = [ pkgs.go pkgs.findutils ];
          text = ''
            find . -name '*.go' -not -path './.git/*' -print0 | xargs -0 -r gofmt -w
          '';
        });

      devShells = eachSystem (system: pkgs: {
        default = pkgs.mkShell {
          shellHook = ''
            # Set here the env vars you want to be available in the shell
          '';
          hardeningDisable = [ "all" ];

          # nodejs builds the embedded admin SPA (go generate ./cmd/amber-store);
          # sqlc regenerates refstore/internal/refsdb (go generate ./refstore)
          packages = with pkgs; [ go nodejs python3 sqlc ];
        };
      });
    };
}
