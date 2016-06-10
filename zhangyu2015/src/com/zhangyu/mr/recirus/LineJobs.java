    //子任务1的执行代码
		Configuration conf1 = new Configuration() ;
		Job job1 = new Job(conf1,"jobName1") ;
		job1.setJarByClass(jobClass1);
		//...
		FileInputFormat.addInputPath(job1, new Path(inpath1)) ;
		FileOutputFormat.setOutputPath(job1, new Path(outpath1));
		job1.waitForCompletion(true) ;
		//子任务2的执行代码
		Configuration conf2 = new Configuration() ;
		Job job2 = new Job(conf2,"jobName2") ;
		job2.setJarByClass(jobClass2);
		//...
		FileInputFormat.addInputPath(job2, new Path(outpath1)) ;
		FileOutputFormat.setOutputPath(job2, new Path(outpath2));
		job2.waitForCompletion(true) ;
		//子任务3的执行代码
		Configuration conf3 = new Configuration() ;
		Job job3 = new Job(conf3,"jobName3") ;
		Job3.setJarByClass(jobClass3);
		//...
		FileInputFormat.addInputPath(job3, new Path(outpath2)) ;
		FileOutputFormat.setOutputPath(job3, new Path(outpath3));
		job3.waitForCompletion(true) ;




//******************JobControl******************************************************

Configuration conf = new Configuration() ;
		//第一部分：作业配置，注意只做配置不做运行
		//job1的配置
		ControlledJob joba = new ControlledJob(conf) ;
		Job job1 = new Job(conf,"jobName1") ;
		job1.setJarByClass(jobClass1);
		//...
		joba.setJob(job1);
		//job2的配置
		ControlledJob jobb = new ControlledJob(conf) ;
		Job job2 = new Job(conf,"jobName2") ;
		job2.setJarByClass(jobClass2);
		//...
		jobb.setJob(job2);
		//job3的配置
		ControlledJob jobc = new ControlledJob(conf) ;
		Job job3 = new Job(conf,"jobName3") ;
		job3.setJarByClass(jobClass3);
		//...
		jobc.setJob(job3);
		
		//第二部分：依赖关系配置
		jobb.addDependingJob(joba) ;
		jobc.addDependingJob(jobb) ;
		
		//第三部分：JobControl配置
		JobControl jc = new JobControl("abc") ;
		jc.addJob(joba) ;
		jc.addJob(jobb) ;
		jc.addJob(jobc) ;
//*************************************************************************************
