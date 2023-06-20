package tools.java_api;
import java.time.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;

public class test {

    public static void deleteFolder(File folder) {
        if (folder.exists()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteFolder(file);
                    } else {
                        file.delete();
                    }
                }
            }
            folder.delete();
            System.out.println("Folder deleted: " + folder.getAbsolutePath());
        }
    }

    public static void main(String[] args) throws KuzuObjectRefDestroyedException {
        
        String folderPath = "java_api_test_db";
        deleteFolder(new File(folderPath));

        BufferedReader reader;
        KuzuDatabase db = new KuzuDatabase("java_api_test_db", 0);
        KuzuConnection conn = new KuzuConnection(db);
        try {
			reader = new BufferedReader(new FileReader("./../../dataset/tinysnb/schema.cypher"));
			String line = reader.readLine();

			while (line != null) {
				conn.query(line);
				line = reader.readLine();
			}

            reader.close();

            reader = new BufferedReader(new FileReader("./../../dataset/tinysnb/copy.cypher"));
            line = reader.readLine();

            while (line != null) {
				conn.query(line);
				line = reader.readLine();
			}

			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.age ORDER BY a.fName");

        KuzuFlatTuple row = result.getNext();
        System.out.println(row);
        row.destroy();

        row = result.getNext();
        row.destroy();

        result.destroy();


        KuzuValue value = new KuzuValue(Duration.ofMillis(31800000003L));
        Duration interval = value.getValue();
        

    }
}
