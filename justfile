_default:
  just --list -u

alias r := ready
alias qc := quick-commit
alias rg := reset-git

# Installs the tools needed to develop
install-tools:
	cargo install cargo-binstall
	cargo binstall taplo-cli

# Upgrades the tools needed to develop
upgrade-tools:
	cargo install cargo-binstall --force
	cargo binstall taplo-cli

format:
    cargo fmt
    taplo format

lint:
    cargo clippy --all-targets --all-features -- -D warnings

lint-fix:
    cargo clippy --all-targets --all-features --fix --allow-dirty -- -D warnings

ready: format lint-fix

quick-commit:
    just ready
    git add -A
    git commit -m "progress"
    git push

clear-branches:
    git branch --merged | egrep -v "(^\\*|main)" | xargs git branch -d

reset-git:
    git checkout main
    git pull
    just clear-branches

