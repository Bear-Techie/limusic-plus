// Preserve existing pinned shortcuts and URL handlers after renaming the real executable.
using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Text;
using System.Windows.Forms;

[assembly: AssemblyTitle("Limusic Plus")]
[assembly: AssemblyProduct("Limusic Plus")]

internal static class CompatibilityLauncher {
    // ProcessStartInfo on .NET Framework takes a command line, not an argument list.
    private static string Quote(string argument) {
        var result = new StringBuilder("\"");
        int slashes = 0;
        foreach (char character in argument) {
            if (character == '\\') { slashes++; continue; }
            if (character == '"') result.Append('\\', slashes * 2 + 1).Append('"');
            else result.Append('\\', slashes).Append(character);
            slashes = 0;
        }
        return result.Append('\\', slashes * 2).Append('"').ToString();
    }

    [STAThread]
    private static int Main(string[] args) {
        try {
            string root = AppDomain.CurrentDomain.BaseDirectory;
            string executable = Path.Combine(root, "Limusic Plus.exe");
            if (!File.Exists(executable)) throw new FileNotFoundException("Please reinstall Limusic Plus.");
            var arguments = new StringBuilder();
            foreach (string argument in args) {
                if (arguments.Length != 0) arguments.Append(' ');
                arguments.Append(Quote(argument));
            }
            Process.Start(new ProcessStartInfo {
                FileName = executable,
                Arguments = arguments.ToString(),
                WorkingDirectory = root,
                UseShellExecute = false,
                CreateNoWindow = true,
                WindowStyle = ProcessWindowStyle.Hidden
            });
            return 0;
        } catch (Exception error) {
            MessageBox.Show(error.Message, "Limusic Plus", MessageBoxButtons.OK, MessageBoxIcon.Error);
            return 1;
        }
    }
}
