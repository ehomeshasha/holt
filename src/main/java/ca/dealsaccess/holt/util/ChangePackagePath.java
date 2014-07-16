package ca.dealsaccess.holt.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.google.common.io.Files;

public class ChangePackagePath {
	
	private static final String path = "/home/hadoop-user/scout_workspace/holt/src/main/java";
	
	private static final String find = "fks";
	
	private static final String replacement = "holt";
	
	public static void main(String[] args) {
		ChangePackagePath self = new ChangePackagePath();
		self.run();
		
		
	}

	private void run() {
		ChangePackagePathFilter filter = new ChangePackagePathFilter();
		new File(path).listFiles(filter);
		
	}
	
	private class ChangePackagePathFilter implements FileFilter {

		@Override
		public boolean accept(File f) {
			if(f.isDirectory()) {
				f.listFiles(this);
			} else {
				
				String absolutePath = f.getAbsolutePath();
				String tmpPath = absolutePath+".tmp";
				File tmpFile = new File(tmpPath);
				
				try {
					BufferedReader br = new BufferedReader(new FileReader(f));
					BufferedWriter bw = new BufferedWriter(new FileWriter(tmpFile));
					
					String line = null;
					while((line = br.readLine()) != null) {
						if(line.startsWith("package") || line.startsWith("import")) {
							String newline = line.replace(find, replacement);
							bw.write(newline);
						} else {
							bw.write(line);
						}
						bw.write('\n');
					}
					bw.close();
					br.close();
					Files.move(tmpFile, f);
					return true;
				} catch (IOException e) {
					return false;
				}
			}
			return true;
		}
		
	}
	
}
