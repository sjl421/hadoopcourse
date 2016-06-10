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
