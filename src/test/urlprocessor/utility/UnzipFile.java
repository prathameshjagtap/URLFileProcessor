package test.urlprocessor.utility;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;

/**
 * <a href="https://www.baeldung.com/java-compress-and-uncompress">Ref: Zipping and Unzipping in Java</a> <br/>
 * <a href="https://stackoverflow.com/questions/315618/how-do-i-extract-a-tar-file-in-java/7556307#7556307">Ref: Stackoverflow</a>
 * @author baeldung.com & Dan Borza
 *
 */
public class UnzipFile {

	public static void unzip(String fileToZip, String destinationDir) throws IOException {
		String fileZip = fileToZip;
        File destDir = new File(destinationDir);
        byte[] buffer = new byte[1024];
        ZipInputStream zis = new ZipInputStream(new FileInputStream(fileZip));
        ZipEntry zipEntry = zis.getNextEntry();
        while (zipEntry != null) {
            File newFile = newFile(destDir, zipEntry);
            
            if(zipEntry.isDirectory()){
            	newFile.mkdirs();
            	newFile.mkdir();
            }
            else{
                FileOutputStream fos = new FileOutputStream(newFile);
                int len;
                while ((len = zis.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }
                fos.close();
            }
            
            zipEntry = zis.getNextEntry();
        }
        zis.closeEntry();
        zis.close();
	}
	
	private static File newFile(File destinationDir, ZipEntry zipEntry) throws IOException {
        File destFile = new File(destinationDir, zipEntry.getName());
         
        String destDirPath = destinationDir.getCanonicalPath();
        String destFilePath = destFile.getCanonicalPath();
         
        if (!destFilePath.startsWith(destDirPath + File.separator)) {
            throw new IOException("Entry is outside of the target dir: " + zipEntry.getName());
        }
         
        return destFile;
    }
	
	/**
	 * Ungzip an input file into an output file.
	 * <p>
	 * The output file is created in the output folder, having the same name
	 * as the input file, minus the '.gz' extension. 
	 * 
	 * @param inputFile     the input .gz file
	 * @param outputDir     the output directory file. 
	 * @throws IOException 
	 * @throws FileNotFoundException
	 *  
	 * @return  The {@File} with the ungzipped content.
	 */
	public static File unGzip(final File inputFile, final File outputDir) throws FileNotFoundException, IOException {

	    final File outputFile = new File(outputDir, inputFile.getName().substring(0, inputFile.getName().length() - 3));

	    final GZIPInputStream in = new GZIPInputStream(new FileInputStream(inputFile));
	    final FileOutputStream out = new FileOutputStream(outputFile);

	    IOUtils.copy(in, out);

	    in.close();
	    out.close();

	    return outputFile;
	}
}
