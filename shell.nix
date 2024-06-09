{ pkgs ? import <nixpkgs> { } }:
pkgs.mkShell {
  buildInputs = [
    pkgs.ocaml
    pkgs.ocamlPackages.base
    pkgs.ocamlPackages.csv
  ];
}
