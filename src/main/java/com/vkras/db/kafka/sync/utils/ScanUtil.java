package com.vkras.db.kafka.sync.utils;

import com.vkras.db.kafka.sync.annotation.SynchronizedTable;
import com.vkras.db.kafka.sync.config.SynchronizedProperties;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

@Component
public class ScanUtil {
    private final SynchronizedProperties properties;

    public ScanUtil(SynchronizedProperties synchronizedProperties) {
        this.properties = synchronizedProperties;
    }

    /**
     * I want to add scan of all classloader for future feature (generating client library)
     * finding classes by properties list or via {@link ClassLoader}
     * @return List of Classes annotated with {@link SynchronizedTable} or {@link com.vkras.db.kafka.sync.annotation.ExternalTable}
     */
    public List<Class<?>> findClasses(Class annotation){
        URLClassLoader classLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
        if (StringUtils.isEmpty(properties.getScanPackage())){
            URL[] urls = classLoader.getURLs();
            return checkUrls(urls, classLoader, annotation);
        }
        else {
            try {
                URL[] urls = getDirectoryByPackageName(classLoader);
                return checkUrls(urls, classLoader, annotation);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * get URL[] by package name
     * @param classLoader - system ClassLoader
     * @return - array ofURL
     * @throws IOException - getResources produce exception
     */
    private URL[] getDirectoryByPackageName(URLClassLoader classLoader) throws IOException {
        Enumeration<URL> resources = classLoader.getResources(properties.getScanPackage());
        ArrayList<URL> listOfURl = new ArrayList<>();
        while (resources.hasMoreElements()) {
            listOfURl.add(resources.nextElement());
        }
        URL[] urls = new URL[listOfURl.size()];
        return listOfURl.toArray(urls);
    }

    /**
     * check URL:
     * if File - checking for annotation,
     * if package - recursive scan
     * @param urls - List of files/folders
     * @param classLoader - System ClassLoader
     * @return - list of {@link Class} annotated with {@link SynchronizedTable}
     */
    private List<Class<?>> checkUrls(URL[] urls,
                                     URLClassLoader classLoader,
                                     Class annotation){
        List<Class<?>> resultUrls = new ArrayList<>();
        for (URL url : urls) {
            File file = new File(url.getFile());
            if (file.isDirectory()) {
                URL[] childUrls = (URL[]) Arrays.stream(file.listFiles())
                        .map(File::toURI)
                        .map(uri -> {
                            try {
                                return uri.toURL();
                            } catch (MalformedURLException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .toArray();
                resultUrls.addAll(checkUrls(childUrls, classLoader, annotation));
            } else {
                resultUrls.add(scanClassFile(file, annotation));
            }
        }
        return resultUrls;
    }

    /**
     * Check is Class containts {@link SynchronizedTable}
     * @param file - file inside folders
     * @return - Class if exists, or null
     */
    private Class<?> scanClassFile(File file, Class annotation){
        String path = file.getAbsolutePath();
        String className = path.substring(path.indexOf("classes") + 8, path.lastIndexOf("."));
        className = className.replace("/", ".");
        try {
            Class<?> cls = Class.forName(className);
            if (cls.isAnnotationPresent(annotation)) {
                return cls;
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}
