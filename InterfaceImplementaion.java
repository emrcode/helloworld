package sample_project;

public class InterfaceImplementaion {
	
	public static void main (String[] args){
		
		 ClassSuper supobj = new ClassSuper(30,40);
		
		 ClassSub subobj = new ClassSub(10,20,90);
		 
		 ClassSub subobj1 = new ClassSub(70,80,100);
		
		 supobj.printStatement();
		 
		 subobj.printStatement();
		 
		 supobj = subobj1;
		 
		 supobj.printStatement();
		 
		 subobj = (ClassSub) supobj;
		 
		 subobj.printStatement();
	}
	

}
