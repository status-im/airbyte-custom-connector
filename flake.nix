{
  description = "shell for Airbyte connectors";

  inputs.nixpkgs.url = "github:nixos/nixpkgs/nixos-25.05";

  outputs = { self, nixpkgs }:
    let
      stableSystems = ["x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin"];
      forAllSystems = nixpkgs.lib.genAttrs stableSystems;
      pkgsFor = nixpkgs.lib.genAttrs stableSystems (
        system: import nixpkgs { inherit system; config.allowUnfree = true; }
      );
    in rec {
      devShells = forAllSystems (system: let
        pkgs = pkgsFor.${system};
      in {
        default = let
          pythonPkgs = pkgs.python312.withPackages (
            _: with (pkgs.python312Packages); [
              ipython pyyaml jinja2 PyGithub
            ]
          );
        in pkgs.mkShellNoCC {
          packages = with pkgs.buildPackages; [
            # misc
            git jq silver-searcher direnv
            pythonPkgs
            ruff
          ];

          shellHook = ''
            echo "Good hacking."
          '';
        };
      });
    };
}
