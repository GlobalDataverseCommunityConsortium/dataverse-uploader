/** *****************************************************************************
 * Copyright 2016 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ***************************************************************************** */
package org.sead.uploader.clowder;

import java.net.PasswordAuthentication;

/**
 * @author Jim
 *
 */
public class SEAD2UPLogin {

    static private PasswordAuthentication pAuth = null;

    /**
     * @param args
     */
    public static void main(String[] args) {
        getPasswordAuthentication();
        System.out.println("Input: Username: " + pAuth.getUserName() + ", Password: " + pAuth.getPassword().toString());
    }

    static PasswordAuthentication getPasswordAuthentication() {
        if (pAuth == null) {
            System.out
                    .println("Enter the username/password for you sead2 account.\n");
            System.out.println("Username: ");

            String username = null;
            char[] password = null;
            username = System.console().readLine();

            System.out.println("Password: ");
            password = System.console().readPassword();
            if (username != null && password.length != 0) {
                pAuth = new PasswordAuthentication(username, password);
            }
        }
        return pAuth;
    }
}
