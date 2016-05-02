/*
 * Author: 609349708 (Abhinav Meghmala),
 * 			609298143 (Manish Bajaj),
 * 			609504664 (Prabhu Om)
 */

package com.bt.dataintegration.test.db;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

public class TestRunner {

	private static Result result;
	private static int count = 0;

	public static void main(String[] args) {

		//Run Test cases for DBAuth
		result = JUnitCore.runClasses(TestCaseDBAuth.class);

		//Counting the failed assertions
		for (Failure failure : result.getFailures()) {
			count++;
			System.out.println(failure.toString());
		}

		System.out.println("Failure counts for Hive: " + count);		
	}
}
