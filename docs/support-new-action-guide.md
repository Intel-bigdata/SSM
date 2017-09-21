1. **Implement a sub class of `SmartAction` Class**


     All actions in SSM are implemented as a sub class of `SmartAction` or `HdfsAction` if it's HDFS related.  Generally each action should have there own implementations for following 2 functions,
   
     - a. public void init(Map<String, String> args)
  
        Initialize action instance, handles all input parameters
  
     - b. protected abstract void execute() throws Exception
   
        Action exuction body. All action steps should be finished in this fuction. 
      
   
2. **Register action class so that SSM knows the new defined action**

 
     - a. If the new action class is a sub class of `SmartAction`, register itself to `AbstractActionFactory`

        `addAction(NewActionClass.class)`
	
     - b. If the new action class is a sub class of `HdfsAction`, register itself ot `HdfsActionFactory`
	
	      `addAction(NewActionClass.class)`
      

3. **Define the action string used in rule syntax**

    
    For example, user want to use "distcp" as the action name in rule syntax, then add the following ahead of action class definition,
	
	    @ActionSignature(
         actionId = "distcp",
         displayName = "distcp",
         usage = "-file " + "$file" + " -delete " +  "-override " + "-target " + "$path"; 
         )
       
  	User need to define all supported parameter in the `usage` field and handle them in the `init` function. The first parameter `-file $file` is a fixed and mandatory parameter for all actions. 
	
	  When rule engine create a new instance of action, it will call action's `init(Map<String, String> args)` function, pass the matched file as the value of first parameter `-file`. 

	  User can refer to `WriteFileAction` class which is a good example. 
    

 4. **Try use new action in rule**   
 
 
    After above steps, rebuild and redeploy SSM. New action can be used in rule like 

      `file: path matches "/test/*" and age > 30d | distcp -delete -override -target s3a://nutch/`
