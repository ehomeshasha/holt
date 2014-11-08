package ca.dealsaccess.holt;

import static org.junit.Assert.*;

import java.util.TimeZone;

import org.junit.Test;


public class AppTest {
	/*@Test
	public void testAdd() {
		AppTest test = new AppTest();
		double result = test.add(10, 50);
		assertEquals(60, result ,0);
	}
	public double add(double num1, double num2) {
		return num1 + num2;
	}*/
	@Test
	public void printTimezone() {
		TimeZone timezone = TimeZone.getDefault();
		System.out.println(timezone.getID());
	}
}
