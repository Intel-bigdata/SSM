Enable Web Authentication
-------------------------
1. **Go to SSM home directory**

2. **conf/zeppeline-site.xml**
   
   For property `zeppelin.anonymous.allowed`, change it's value from default `true` to `false`.

3. **conf/shiro.ini**
   
   * **[urls]** section
   
      Please comment the below line by adding "#" to disallow anonymous user access all paths.

      `/** = anon`
      
      Please uncomment the below two lines by removing "#" to only allow authenticated user access all paths.

      `/** = authc`

      Note: this is URL-based security configuration. Here, we enforce all urls to be authenticated.

4. **restart SSM service**

   Visit `http://ssm-server-ip:7045` to open the UI. 

For more information about security configuration,
please refer to [official doc](`https://zeppelin.apache.org/docs/0.7.2/security/shiroauthentication.html`

Note
----
* If authentication is enabled, please login with the default credential (admin/ssm@123). We recommend to change the password in the first login.
* To allow anonymous user login, please do the above setting conversely. Enabling anonymous user to login without authentication can facilitate testing SSM.