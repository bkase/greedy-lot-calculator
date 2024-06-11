{
  description = "A very basic flake";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      # Define systems for Linux and macOS
      system = "aarch64-darwin";
      pkgs = import nixpkgs {
        inherit system;
      };
    in
    {
      devShells.aarch64-darwin.default = pkgs.mkShell
        {
          buildInputs = [
            pkgs.ocaml
            pkgs.ocamlPackages.dune_3
            pkgs.ocamlPackages.base
            pkgs.ocamlPackages.core
            pkgs.ocamlPackages.core_kernel
            pkgs.ocamlPackages.containers
            pkgs.ocamlPackages.csv
            pkgs.ocamlPackages.utop
            pkgs.ocamlPackages.ocaml-lsp
            pkgs.ocamlPackages.ppx_deriving
            pkgs.ocamlPackages.ppx_inline_test
            pkgs.ocamlPackages.bignum
            pkgs.ocamlPackages.ocamlformat
          ];
          shellHook = ''
            echo "Welcome to the OCaml development environment on ${system}!"
          '';
        };
    };

}
