package fr.isae.iqas.tools;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

/**
 * In order to impose tight security restrictions on untrusted classes but
 * not on trusted system classes, we have to be able to distinguish between
 * those types of classes. This is done by keeping track of how the classes
 * are loaded into the system. By definition, any class that the interpreter
 * loads directly from the CLASSPATH is trusted. This means that we can't
 * load untrusted code in that way--we can't load it with Class.forName().
 * Instead, we create a ClassLoader subclass to load the untrusted code.
 * This one loads classes from a specified directory (which should not
 * be part of the CLASSPATH).
 */

public class PluginClassLoader extends ClassLoader {
    private List classRepository;//class repository where findClass performs its search

    public PluginClassLoader(ClassLoader parent, String searchPath) {
        super(parent);
        initLoader(searchPath);
    }

    public PluginClassLoader(String searchPath) {
        super(PluginClassLoader.class.getClassLoader());
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
        //else
        throw new ClassNotFoundException(className);
    }

    /*
       A private method that loads binary class file data from the classRepository.
    */
    private byte[] loadFromCustomRepository(String classFileName) throws ClassNotFoundException {
        Iterator dirs = classRepository.iterator();
        byte[] classBytes = null;
        while (dirs.hasNext()) {
            String dir = (String) dirs.next();
            //replace '.' in the class name with File.separatorChar & append .class to the name
            //String classFileName = className;
            classFileName.replace('.', File.separatorChar);
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
                //return null
                System.out.println("IOException raised while reading class file data");
                ex.printStackTrace();
                return null;
            }
        }
        return classBytes;
    }

    private void initLoader(String searchPath) {
        //userClassPath is passed in as a string of directories/jar files separated
        //by the File.pathSeparator
        classRepository = new ArrayList();
        if ((searchPath != null) && !(searchPath.equals(""))) {
            StringTokenizer tokenizer = new StringTokenizer(searchPath, File.pathSeparator);
            while (tokenizer.hasMoreTokens()) {
                classRepository.add(tokenizer.nextToken());
            }
        }

    }

    /** A convenience method that calls the 2-argument form of this method */
    /*public Class loadClass (String name) throws ClassNotFoundException {
        return loadClass(name, true);
    }*/

    /**
     * This is one abstract method of ClassLoader that all subclasses must
     * define. Its job is to load an array of bytes from somewhere and to
     * pass them to defineClass(). If the resolve argument is true, it must
     * also call resolveClass(), which will do things like verify the presence
     * of the superclass. Because of this second step, this method may be called to
     * load superclasses that are system classes, and it must take this into account.
     */
    /*public Class loadClass (String classname, boolean resolve) throws ClassNotFoundException {
        try {
            // Our ClassLoader superclass has a built-in cache of classes it has
            // already loaded. So, first check the cache.
            Class c = findLoadedClass(classname);

            // After this method loads a class, it will be called again to
            // load the superclasses. Since these may be system classes, we've
            // got to be able to load those too. So try to load the class as
            // a system class (i.e. from the CLASSPATH) and ignore any errors
            if (c == null) {
                try { c = findSystemClass(classname); }
                catch (Exception ex) {}
            }

            // If the class wasn't found by either of the above attempts, then
            // try to load it from a file in (or beneath) the directory
            // specified when this ClassLoader object was created. Form the
            // filename by replacing all dots in the class name with
            // (platform-independent) file separators and by adding the ".class" extension.
            if (c == null) {
                // Figure out the filename
                System.out.println(classname);
                System.out.println(directory);
                String filename = classname.replace('.',File.separatorChar)+".class";
                System.out.println(filename);

                // Create a File object. Interpret the filename relative to the
                // directory specified for this ClassLoader.
                File f = new File(directory, filename);

                // Get the length of the class file, allocate an array of bytes for
                // it, and read it in all at once.
                int length = (int) f.length();
                byte[] classbytes = new byte[length];
                DataInputStream in = new DataInputStream(new FileInputStream(f));
                in.readFully(classbytes);
                in.close();

                // Now call an inherited method to convert those bytes into a Class
                c = defineClass(classname, classbytes, 0, length);
            }

            // If the resolve argument is true, call the inherited resolveClass method.
            if (resolve) resolveClass(c);

            // And we're done. Return the Class object we've loaded.
            return c;
        }
        // If anything goes wrong, throw a ClassNotFoundException error
        catch (Exception ex) { throw new ClassNotFoundException(ex.toString()); }
    }*/
}
