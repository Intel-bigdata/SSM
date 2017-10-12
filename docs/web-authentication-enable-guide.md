Steps to enable authentication on WebUI
---------------------------------------------------------------------------------
1. **Go to the SSM installation directory**

2. **conf/zeppeline-site.xml**
   
   Find property `zeppelin.anonymous.allowed`, change it's value from default  `false` to `true`
   
 
3. **conf/shiro.ini.template**
   
   * rename file to `shiro.ini`
   
   * **[users]** section
   
      define supported user name and password. It follows the `username = password, role` format.
   
      Here is an example,
     
      ```
      admin = intel@intel, admin
      ssmoperator = operator@operation, operator    
      ```
    
   * **[roles]** section
   
      define support roles. Here is the example, 
   
      ```
      operator = *
     admin = *    
     ```
   
   * **[urls]** section
   
      comment out these two lines by put "#" at the begining of the line
      
      `/api/version = anon`
      
      `/** = anon`
      
      uncomment theset two lines by remove the heading "#"
      
      `/api/interpreter/** = authc, roles[admin]`
      
      `/** = authc`
    
4. **restart SSM service**

   Visit `http://ssm-server-ip:7045` to open the UI. 




   
For more information about security configuration, please refer to official document   
   
   `https://zeppelin.apache.org/docs/0.7.2/security/shiroauthentication.html`
   
or other shiro offical documents. 





   
