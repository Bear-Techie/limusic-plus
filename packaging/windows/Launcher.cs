using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Text;
using System.Runtime.InteropServices;
using System.Windows.Forms;
using System.Reflection;
using System.Security.Cryptography;
using System.Text.RegularExpressions;

[assembly: AssemblyTitle("Limusic Plus")]
[assembly: AssemblyProduct("Limusic Plus")]

internal static class Launcher {
    private const string AppFileName = "Limusic Plus.exe";
    private static void Log(string message) {
        try {
            string data = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "LimusicRelay");
            Directory.CreateDirectory(data);
            File.AppendAllText(Path.Combine(data, "launcher-native.log"), DateTime.UtcNow.ToString("u") + " " + message + Environment.NewLine);
        } catch (Exception) { } // Logging must not prevent startup.
    }
    private delegate bool WindowCallback(IntPtr window, IntPtr parameter);
    [DllImport("user32.dll")]
    private static extern bool EnumWindows(WindowCallback callback, IntPtr parameter);
    [DllImport("user32.dll", CharSet=CharSet.Unicode)]
    private static extern int GetWindowText(IntPtr window, StringBuilder title, int length);
    [DllImport("user32.dll")]
    private static extern uint GetWindowThreadProcessId(IntPtr window, out uint processId);
    [DllImport("user32.dll")]
    private static extern bool ShowWindowAsync(IntPtr window, int command);
    [DllImport("user32.dll")]
    private static extern bool SetForegroundWindow(IntPtr window);
    [DllImport("user32.dll")]
    private static extern bool IsWindowVisible(IntPtr window);

    private static bool RestoreExisting(string root) {
        foreach (Process process in Process.GetProcessesByName("Limusic Plus")) {
            using (process) {
                try {
                    if (!String.Equals(process.MainModule.FileName, Path.Combine(root, AppFileName), StringComparison.OrdinalIgnoreCase)) continue;
                    IntPtr main = IntPtr.Zero, mini = IntPtr.Zero;
                    EnumWindows(delegate(IntPtr window, IntPtr unused) {
                        uint owner;
                        GetWindowThreadProcessId(window, out owner);
                        if (owner != process.Id) return true;
                        var title = new StringBuilder(256);
                        GetWindowText(window, title, title.Capacity);
                        if (title.ToString() == "Limusic Plus") main = window;
                        if (title.ToString() == "Limusic Plus Mini Player" && IsWindowVisible(window)) mini = window;
                        return true;
                    }, IntPtr.Zero);
                    IntPtr target = mini != IntPtr.Zero ? mini : main;
                    Log("Restore app PID " + process.Id + ", window " + target.ToInt64());
                    if (target != IntPtr.Zero) {
                        ShowWindowAsync(target, 9); // SW_RESTORE, including a hidden tray window.
                        SetForegroundWindow(target);
                    }
                    return true;
                } catch (InvalidOperationException) { }
                catch (System.ComponentModel.Win32Exception) { } // An unrelated elevated copy.
            }
        }
        return false;
    }
    [STAThread]
    private static int Main(string[] args) {
        if (args.Length != 0 && args[0] == "--apply-update") {
            try { return ApplyUpdate(args); }
            catch (Exception error) { Log("Update failed: " + error.Message); return 1; }
        }
        try {
            string root = AppDomain.CurrentDomain.BaseDirectory;
            foreach (string file in new[] { Path.Combine(root, AppFileName), Path.Combine(root, "limusic-sync.exe"), Path.Combine(root, "libmpv-2.dll") }) {
                if (!File.Exists(file)) throw new FileNotFoundException("Please reinstall Limusic Plus. A required file is missing: " + file);
            }
            if (args.Length == 1 && args[0] == "--check") return 0;
            string invite = null;
            if (args.Length != 0) {
                if (args.Length != 1 || !Regex.IsMatch(args[0], @"\Alimusic-plus://join/[0-9a-f]{64}/[a-z0-9]{6,32}\z",
                        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)) return 1;
                invite = args[0];
            }
            Log("Shortcut launch");
            if (invite != null) {
                // Forward to Tauri's single-instance handler even if the main window already exists.
                StartApp(root, invite);
                return 0;
            }
            using (var gate = new Mutex(false, @"Local\LimusicPlusLauncher")) {
                bool entered;
                try { entered = gate.WaitOne(0); }
                catch (AbandonedMutexException) { entered = true; }
                if (!entered) return 0;
                try {
                    if (RestoreExisting(root)) return 0;
                    StartApp(root, null);
                    return 0;
                } finally { gate.ReleaseMutex(); }
            }
        } catch (Exception error) {
            Log(error.Message);
            if (args.Length == 1 && args[0] == "--check") return 1;
            MessageBox.Show(error.Message, "Limusic Plus", MessageBoxButtons.OK, MessageBoxIcon.Error);
            return 1;
        }
    }

    private static int ApplyUpdate(string[] args) {
        int pid;
        if (args.Length != 5 || !Int32.TryParse(args[1], out pid) || pid <= 0 ||
            !Regex.IsMatch(args[3], @"\A[0-9a-f]{64}\z")) return 1;
        var stage = new DirectoryInfo(AppDomain.CurrentDomain.BaseDirectory);
        var temp = new DirectoryInfo(Path.GetTempPath());
        if (!stage.Name.StartsWith("limusic-plus-update-", StringComparison.Ordinal) ||
            !String.Equals(stage.Parent.FullName, temp.FullName.TrimEnd('\\'), StringComparison.OrdinalIgnoreCase)) return 1;
        string root = Path.GetFullPath(args[2]);
        string setup = Path.Combine(stage.FullName, "setup.exe");
        try {
            // Hold the installer open read-only until it has run, so the verified bytes are the ones executed.
            using (var stream = File.Open(setup, FileMode.Open, FileAccess.Read, FileShare.Read))
            using (var sha256 = SHA256.Create()) {
                string actual = BitConverter.ToString(sha256.ComputeHash(stream)).Replace("-", "").ToLowerInvariant();
                if (!String.Equals(args[3], actual, StringComparison.Ordinal)) return 1;
                var version = FileVersionInfo.GetVersionInfo(setup);
                var fileVersion = new Version(version.FileMajorPart, version.FileMinorPart, version.FileBuildPart);
                if (!String.Equals(args[4], fileVersion.ToString(), StringComparison.Ordinal)) return 1;
                Process parent = null;
                try { parent = Process.GetProcessById(pid); }
                catch (ArgumentException) { } // The app has already quit.
                if (parent != null) {
                    using (parent) {
                        try {
                            if (!parent.HasExited &&
                                (!String.Equals(parent.MainModule.FileName, Path.Combine(root, AppFileName), StringComparison.OrdinalIgnoreCase) ||
                                !parent.WaitForExit(60000))) return 1;
                        } catch (InvalidOperationException) {
                            if (!parent.HasExited) throw;
                        } catch (System.ComponentModel.Win32Exception) {
                            if (!parent.HasExited) throw;
                        }
                    }
                }
                if (!File.Exists(Path.Combine(root, "unins000.exe"))) return 1;
                var start = new ProcessStartInfo {
                    FileName = setup,
                    Arguments = "/VERYSILENT /SUPPRESSMSGBOXES /SP- /NORESTART /NOCLOSEAPPLICATIONS /DIR=\"" + root + "\"",
                    WorkingDirectory = stage.FullName,
                    UseShellExecute = false,
                    CreateNoWindow = true,
                    WindowStyle = ProcessWindowStyle.Hidden
                };
                using (var installer = Process.Start(start)) {
                    installer.WaitForExit();
                    Log("Update installer exited with code " + installer.ExitCode);
                    return installer.ExitCode;
                }
            }
        } finally {
            try { File.Delete(setup); } catch (IOException) { }
            catch (UnauthorizedAccessException) { }
            try { File.Delete(Assembly.GetExecutingAssembly().Location); stage.Delete(); } catch (IOException) { }
            catch (UnauthorizedAccessException) { } // Windows can keep this runner mapped until it exits.
        }
    }

    private static void StartApp(string root, string invite) {
        var start = new ProcessStartInfo {
            FileName = Path.Combine(root, AppFileName),
            Arguments = invite == null ? "" : "\"" + invite + "\"",
            WorkingDirectory = root,
            UseShellExecute = false,
            CreateNoWindow = true,
            WindowStyle = ProcessWindowStyle.Hidden
        };
        // A browser originally opened by Limusic can inherit the update/restart marker.
        // An explicit room link must still be handled on a cold launch.
        if (invite != null) start.EnvironmentVariables.Remove("LIMUSIC_RESTARTED");
        Process.Start(start);
    }
}
