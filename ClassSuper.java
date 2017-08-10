package sample_project;

public class ClassSuper implements ClassInt{
	private int a;
	private int b;
	
	ClassSuper (int x,int y){
		this.a = x;
		this.b = y;
		
	}
	
	public void printStatement(){
		System.out.println("Inside Super Class");
		System.out.println("the value of a is "+ a +"the value of b is " + b);
	}
}
