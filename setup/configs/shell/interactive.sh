# Source from an interactive Bash or Zsh rc file.
# Optional integrations are enabled only when their commands are available.
case $- in
    *i*) ;;
    *) return ;;
esac

export FZF_DEFAULT_OPTS='--height=40% --layout=reverse --border'
# Use normal ripgrep ignore rules; do not enumerate ignored or hidden files.
if command -v rg >/dev/null 2>&1; then
    export FZF_DEFAULT_COMMAND='rg --files'
    export FZF_CTRL_T_COMMAND='rg --files'
fi

# Optional: copy ripgrep/config to this path before enabling it.
if [ -f "${XDG_CONFIG_HOME:-$HOME/.config}/ripgrep/config" ]; then
    export RIPGREP_CONFIG_PATH="${XDG_CONFIG_HOME:-$HOME/.config}/ripgrep/config"
fi

if command -v zoxide >/dev/null 2>&1; then
    if [ -n "${ZSH_VERSION:-}" ]; then
        eval "$(zoxide init zsh)"
    elif [ -n "${BASH_VERSION:-}" ]; then
        eval "$(zoxide init bash)"
    fi
fi
# fzf shell keybindings vary by package/version: enable using its own docs.
