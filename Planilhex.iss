[Setup]
AppName=Planilhex
AppVersion=1.0.0
AppId={{F1D5E7E1-5F26-4A7B-8D5F-PLANILHEX-2025}}
DefaultDirName={autopf}\Planilhex
DefaultGroupName=Planilhex
OutputDir=dist_installer
OutputBaseFilename=Planilhex_Installer
Compression=lzma
SolidCompression=yes
WizardStyle=modern
SetupIconFile=logo.ico
UninstallDisplayIcon={app}\Planilhex.exe
ArchitecturesInstallIn64BitMode=x64

[Files]
; mantém TUDO após desinstalar
Source: "dist\Planilhex\*"; DestDir: "{app}"; Flags: recursesubdirs ignoreversion uninsneveruninstall
Source: "logo.ico"; DestDir: "{app}"; Flags: ignoreversion uninsneveruninstall
; runtime do WebView2, offline
Source: "third_party\MicrosoftEdgeWebView2RuntimeInstallerX64.exe"; DestDir: "{tmp}"; Flags: ignoreversion

[Tasks]
Name: "desktopicon"; Description: "Criar atalho na área de trabalho"; GroupDescription: "Atalhos:"; Flags: unchecked

[Icons]
Name: "{group}\Planilhex"; Filename: "{app}\Planilhex.exe"; IconFilename: "{app}\logo.ico"
Name: "{commondesktop}\Planilhex"; Filename: "{app}\Planilhex.exe"; IconFilename: "{app}\logo.ico"; Tasks: desktopicon

[Run]
; instala/atualiza WebView2 silencioso (sem internet se você incluir o instalador standalone)
Filename: "{tmp}\MicrosoftEdgeWebView2RuntimeInstallerX64.exe"; Parameters: "/silent /install"; Flags: waituntilterminated
; inicia o app
Filename: "{app}\Planilhex.exe"; Flags: nowait postinstall skipifsilent
