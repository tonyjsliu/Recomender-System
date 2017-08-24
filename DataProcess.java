package recommendSystem;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.URL;

public class DataProcess {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		URL path = DataProcess.class.getResource("input/");
		File folder = new File(path.getFile());
		File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			if (!listOfFiles[i].isFile()) 
				continue;
			BufferedReader br = new BufferedReader(new FileReader(listOfFiles[i]));
			File file = new File(listOfFiles[i].toString().replaceAll("input", "data"));
			file.createNewFile();
			BufferedWriter bw = new BufferedWriter(new FileWriter(file));
			String line = null, movie = null;
			
			while ((line = br.readLine()) != null) {
				if (line.contains(":")) {
					movie = line.substring(0, line.indexOf(':'));
					continue;
				}
				String[] str = line.split(",");
				bw.write(str[0]+","+movie+","+str[1]);
				bw.newLine();
			}
			br.close();
			bw.close();
		}
	}

}
