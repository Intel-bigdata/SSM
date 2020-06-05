Steps to enable authentication on WebUI
---------------------------------------------------------------------------------
1. **Go to the SSM installation directory**

2. **conf/zeppeline-site.xml**
   
   For property `zeppelin.anonymous.allowed`, change it's value from default `true` to `false`.
   
 
3. **conf/shiro.ini**
   
   * **[users]** section
   
      define supported user name and password. It follows the `username = password, role` format.
   
      Here is an example,
     
      ```
      admin = intel@intel, admin
      ssmoperator = operator@operation, operator    
      ```
    
   * **[roles]** section
   
      define support roles. Here is an example,
   
      ```
      operator = *
      admin = *
     ```
   
   * **[urls]** section
   
      comment the below line by adding "#" to disallow anonymous user access.

      `/** = anon`
      
      uncomment the below two lines by removing "#" to only allow authenticated user access.

      `/** = authc`
    
4. **restart SSM service**

   Visit `http://ssm-server-ip:7045` to open the UI. 


For more information about security configuration, please refer to official document   
   
   `https://zeppelin.apache.org/docs/0.7.2/security/shiroauthentication.html`
   
or other shiro official documents.

Note
----
To allow anonymous user login, please do the above setting conversely. Enabling Anonymous user to login
without authentication can facilitate testing SSM.