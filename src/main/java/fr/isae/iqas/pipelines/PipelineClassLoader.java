package fr.isae.iqas.pipelines;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class PipelineClassLoader extends ClassLoader {
    private List classRepository;

    public PipelineClassLoader(ClassLoader parent, String searchPath) {
        super(parent);
        initLoader(searchPath);
    }

    public PipelineClassLoader(String searchPath) {
        super(PipelineClassLoader.class.getClassLoader());
        initLoader(searchPath);
    }

    /**
     * This method overrides the findClass method in the java.lang.ClassLoader
     * Class. The method will be called from the loadClass method of the parent
     * class loader when it is unable to find a class to load.
     * This implementation will look for the class in the class repository.
     *
     * @param className A String specifying the class to be loaded
     * @return A Class object which is loaded into the JVM by the CustomClassLoader
     * @throws ClassNotFoundException if the method is unable to load the class
     */
    protected Class findClass(String className)
            throws ClassNotFoundException {
        byte[] classBytes = loadFromCustomRepository(className);
        if (classBytes != null) {
            return defineClass(className, classBytes, 0, classBytes.length);
        }
        throw new ClassNotFoundException(className);
    }

    /**
     * A private method that loads binary class file data from the classRepository.
     *
     * @param classFileName
     * @return
     * @throws ClassNotFoundException
     */
    private byte[] loadFromCustomRepository(String classFileName) throws ClassNotFoundException {
        Iterator dirs = classRepository.iterator();
        byte[] classBytes = null;
        while (dirs.hasNext()) {
            String dir = (String) dirs.next();
            // Replace '.' in the class name with File.separatorChar & append .class to the name
            classFileName = classFileName.replace('.', File.separatorChar);
            classFileName += ".class";
            try {
                File file = new File(dir, classFileName);
                if (file.exists()) {
                    //read file
                    InputStream is = new FileInputStream(file);
                    classBytes = new byte[is.available()];
                    is.read(classBytes);
                    break;
                }
            } catch (IOException ex) {
                System.out.println("IOException raised while reading class file data");
                ex.printStackTrace();
                return null;
            }
        }
        return classBytes;
    }

    private void initLoader(String searchPath) {
        //User classpath is passed in as a string of directories/jar files separated
        //by the File.pathSeparator
        classRepository = new ArrayList();
        if ((searchPath != null) && !(searchPath.equals(""))) {
            StringTokenizer tokenizer = new StringTokenizer(searchPath, File.pathSeparator);
            while (tokenizer.hasMoreTokens()) {
                classRepository.add(tokenizer.nextToken());
            }
        }
    }
}
