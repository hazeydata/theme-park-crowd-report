# Pencil + Figma Setup (Figma → Live Website)

We recommend the **Pencil** extension (`highagency.pencildev`) to turn the Figma home page design into a live website. Pencil integrates with Cursor/VS Code and supports copy-paste from Figma plus export to HTML/CSS/React.

## Recommended Extension

- **ID**: `highagency.pencildev`
- **Marketplace**: [VS Code](https://marketplace.visualstudio.com/items?itemName=highagency.pencildev) · [Open VSX](https://open-vsx.org/extension/highagency/pencildev)
- **Docs**: [pencil.dev](https://pencil.dev) · [Downloads](https://pencil.dev/downloads)

The project recommends this via [.vscode/extensions.json](../.vscode/extensions.json). Install from the Extensions panel or when Cursor prompts for recommended extensions.

## "Built assets not found. Please build the editor first."

If you see this when opening a `.pen` file (e.g. Pencil’s welcome file):

1. **Reinstall the extension**
   - Uninstall **Pencil** (`highagency.pencildev`)
   - Fully quit Cursor (not just close the window)
   - Reopen Cursor, then install Pencil again from the Extensions panel

2. **Clear extension cache (Windows)**
   - Close Cursor
   - Delete or rename:  
     `%USERPROFILE%\.cursor\extensions\highagency.pencildev-*`  
     (the `*` is the version folder)
   - Reopen Cursor and reinstall Pencil

3. **Try VS Code**
   - Install the same extension in VS Code. If it works there but not in Cursor, the issue is likely Cursor-specific.

4. **Try an older version**
   - In the Extensions panel, open Pencil → **Version History** → **Install Another Version** → pick a slightly older release and test.

5. **Ask Pencil**
   - [Discord](https://discord.gg/Azsk8cnnVp)  
   - [pencil.dev](https://pencil.dev) for support links

## Using Figma designs

- **Copy-paste**: Select a frame/section in Figma → Copy → Paste into a Pencil canvas in Cursor. Vectors, text, and styles come over.
- **Figma plugin**: If you use Pencil’s Figma plugin, export `.pencil` (or equivalent) and open in Pencil in Cursor.

## Alternatives if Pencil doesn’t work

- **Figma for VS Code** (`figma.figma-vscode-extension`): Inspect designs, get code hints.
- **CodeParrot AI** (`CodeParrot-ai.codeParrot`): Figma/images → React, Flutter, HTML, etc.
- **html.to.design**: Web ↔ Figma (different direction, but useful for round-tripping).
