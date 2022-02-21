/**
 * Copyright 2006 The Apache Software Foundation
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package my.hadoop.util;

import java.util.jar.*;

/**
 * A micro-application that prints the main class name out of a jar file.
 * @author Owen O'Malley
 */
public class PrintJarMainClass {

    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            JarFile jar_file = new JarFile(args[0]);
            if (jar_file != null) {
                Manifest manifest = jar_file.getManifest();
                if (manifest != null) {
                    String value = manifest.getMainAttributes().getValue("Main-Class");
                    if (value != null) {
                        System.out.println(value.replaceAll("/", "."));
                        return;
                    }
                }
            }
        } catch (Throwable e) {
            // ignore it
        }
        System.out.println("UNKNOWN");
        System.exit(1);
    }

}
