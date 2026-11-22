#ifndef StageDir
  #error StageDir required
#endif
#ifndef OutputDir
  #error OutputDir required
#endif
#ifndef AppVersion
  #error AppVersion required
#endif
#ifndef AppFileVersion
  #error AppFileVersion required
#endif

[Setup]
AppId={{E19D7FF6-353B-4D23-9207-019E267D6F35}
AppName=Limusic Plus
AppVersion={#AppVersion}
VersionInfoVersion={#AppFileVersion}
VersionInfoTextVersion={#AppVersion}
AppPublisher=Limusic Plus community build
DefaultDirName={localappdata}\Programs\Limusic Plus
DefaultGroupName=Limusic Plus
DisableProgramGroupPage=yes
PrivilegesRequired=lowest
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
MinVersion=10.0
OutputDir={#OutputDir}
OutputBaseFilename=Limusic-Plus-{#AppVersion}-setup-x64
SetupIconFile={#StageDir}\app.ico
UninstallDisplayIcon={app}\Limusic Plus.exe
LicenseFile={#StageDir}\LICENSE
InfoAfterFile={#StageDir}\GETTING-STARTED.txt
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
CloseApplications=yes
AppMutex=com.limusic.plus-sim,Local\LimusicRelayLauncher,Local\LimusicPlusLauncher
RestartApplications=no
ShowLanguageDialog=no

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"
Name: "spanish"; MessagesFile: "compiler:Languages\Spanish.isl"

[CustomMessages]
english.DiscordBroadcasting=Install Discord broadcasting
spanish.DiscordBroadcasting=Instalar transmisión por Discord

[Tasks]
Name: desktopicon; Description: "Create a desktop shortcut"; Flags: checkedonce
Name: discordvoice; Description: "{cm:DiscordBroadcasting}"; Flags: unchecked

[Files]
Source: "{#StageDir}\*"; DestDir: "{app}"; Excludes: "MicrosoftEdgeWebview2Setup.exe,limusic-app.exe,\discord-voice,\discord-voice\*"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "{#StageDir}\discord-voice\*"; DestDir: "{app}\discord-voice"; Flags: ignoreversion recursesubdirs createallsubdirs; Tasks: discordvoice
Source: "{#StageDir}\limusic-app.exe"; DestDir: "{app}"; Flags: ignoreversion; Check: ShouldInstallCompatibilityLauncher
Source: "{#StageDir}\MicrosoftEdgeWebview2Setup.exe"; DestDir: "{tmp}"; Flags: deleteafterinstall; Check: NeedsWebView2

[Icons]
Name: "{userprograms}\Limusic Plus"; Filename: "{app}\LimusicPlus.exe"; WorkingDir: "{app}"; IconFilename: "{app}\Limusic Plus.exe"
Name: "{userdesktop}\Limusic Plus"; Filename: "{app}\LimusicPlus.exe"; WorkingDir: "{app}"; IconFilename: "{app}\Limusic Plus.exe"; Tasks: desktopicon

[Registry]
Root: HKCU; Subkey: "Software\Classes\limusic-plus"; ValueType: string; ValueData: "URL:Limusic Plus room invitation"; Flags: uninsdeletekey
Root: HKCU; Subkey: "Software\Classes\limusic-plus"; ValueType: string; ValueName: "URL Protocol"; ValueData: ""
Root: HKCU; Subkey: "Software\Classes\limusic-plus\DefaultIcon"; ValueType: string; ValueData: """{app}\app.ico"""
Root: HKCU; Subkey: "Software\Classes\limusic-plus\shell\open\command"; ValueType: string; ValueData: """{app}\LimusicPlus.exe"" ""%1"""

[InstallDelete]
Type: files; Name: "{app}\limusic-app.exe"; Check: IsLegacyPlus
Type: filesandordirs; Name: "{app}\discord-voice"

[Run]
Filename: "{app}\LimusicPlus.exe"; Description: "Open Limusic Plus"; Flags: nowait postinstall skipifsilent

[Code]
var
  LegacyPlusInstall, CompatibilityLauncherAllowed: Boolean;

function PrepareToInstall(var NeedsRestart: Boolean): String;
begin
  { Capture existing files before the new launcher is installed. }
  LegacyPlusInstall := FileExists(ExpandConstant('{app}\LimusicPlus.exe'));
  CompatibilityLauncherAllowed := LegacyPlusInstall or not FileExists(ExpandConstant('{app}\limusic-app.exe'));
  Result := '';
end;

function IsLegacyPlus: Boolean;
begin
  Result := LegacyPlusInstall;
end;

function ShouldInstallCompatibilityLauncher: Boolean;
begin
  Result := CompatibilityLauncherAllowed;
end;

procedure RepointExistingAutostart;
var
  Entry, OldExe, NewExe, RunKey: String;
  Offset: Integer;
begin
  RunKey := 'Software\Microsoft\Windows\CurrentVersion\Run';
  OldExe := ExpandConstant('{app}\limusic-app.exe');
  NewExe := ExpandConstant('{app}\Limusic Plus.exe');
  if RegQueryStringValue(HKCU, RunKey, 'Limusic Plus', Entry) then begin
    Offset := Pos(Lowercase(OldExe), Lowercase(Entry));
    if Offset > 0 then begin
      { Keep existing quotes and arguments, and never enable a missing login entry. }
      Delete(Entry, Offset, Length(OldExe));
      Insert(NewExe, Entry, Offset);
      RegWriteStringValue(HKCU, RunKey, 'Limusic Plus', Entry);
    end;
  end;
end;

function NeedsWebView2: Boolean;
var
  Version: String;
  Key: String;
begin
  Key := 'Software\Microsoft\EdgeUpdate\Clients\{F3017226-FE2A-4295-8BDF-00C3A9A7E4C5}';
  Result := not ((RegQueryStringValue(HKLM32, Key, 'pv', Version) and (Version <> '') and (Version <> '0.0.0.0')) or
    (RegQueryStringValue(HKCU, Key, 'pv', Version) and (Version <> '') and (Version <> '0.0.0.0')));
end;

procedure CurStepChanged(CurStep: TSetupStep);
var
  ResultCode: Integer;
begin
  if CurStep = ssPostInstall then RepointExistingAutostart;
  if (CurStep = ssPostInstall) and NeedsWebView2 then begin
    WizardForm.StatusLabel.Caption := 'Installing Microsoft WebView2...';
    if not Exec(ExpandConstant('{tmp}\MicrosoftEdgeWebview2Setup.exe'), '/silent /install', '', SW_HIDE, ewWaitUntilTerminated, ResultCode) then
      RaiseException('Could not start Microsoft WebView2 setup. Run the installer again.');
    if NeedsWebView2 then
      RaiseException('Microsoft WebView2 could not be installed. Connect to the internet and run this installer again.');
  end;
end;
