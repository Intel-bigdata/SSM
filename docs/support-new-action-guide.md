1. **Implement a sub class of `SmartAction` Class**


     All actions in SSM are implemented as a sub class of `SmartAction` or `HdfsAction` if it's HDFS related.  Generally each action should implement the following two functions,
   
     - a. public void init(Map<String, String> args)
  
        Initialize action instance, and handles all input parameters.
  
     - b. protected abstract void execute() throws Exception
   
        Action execution body. All action steps should be provided in this function.
      
   
2. **Register action class so that SSM knows the new defined action**

 
     - a. If the new action class is a sub class of `SmartAction`, register itself to `AbstractActionFactory`

        `addAction(NewActionClass.class)`
	
     - b. If the new action class is a sub class of `HdfsAction`, register itself to `HdfsActionFactory`
	
	      `addAction(NewActionClass.class)`
      

3. **Define the action string used in rule syntax**

    
    For example, if the user wants to use "distcp" as the action name in the rule syntax, add the following content ahead of the action class definition.
	
	    @ActionSignature(
         actionId = "distcp",
         displayName = "distcp",
         usage = "-file " + "$file" + " -delete " +  "-override " + "-target " + "$path"; 
         )
       
  	The user needs to define all supported parameters in the `usage` field and handle them in the `init` function. The first parameter `-file $file` is a fixed mandatory parameter for most actions that
  	require a file path to execute.
	
	  When the rule engine creates a new instance of an action, it will call the action's `init(Map<String, String> args)` function, and pass the matched file as the value of first parameter `-file`.

	  The `WriteFileAction` class is a good example for users to refer to.
    

 4. **Try use new action in rule**   
 
 
    After above steps, rebuild and redeploy SSM. The above new action can be used in rule as follows:

      `file: path matches "/test/*" and age > 30d | distcp -delete -override -target s3a://nutch/`
