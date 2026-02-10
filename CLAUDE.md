# Elisp Style Guide

- Avoid deep `let` → `if` → `let` chains, which significantly reduce readability. Favor flat, linear control flow using `if-let*`, `when-let*`, or similar constructs whenever possible.
