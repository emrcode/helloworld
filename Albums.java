package sample_project;

import java.util.LinkedList;

import com.sun.xml.bind.v2.runtime.reflect.ListIterator;

public class Albums {
	private String Name;
	private LinkedList<Songs> sng;
	public Albums(String name, LinkedList<Songs> sng) {
		Name = name;
		this.sng = sng;
	}
	
	public Albums(String name){
		this.Name = name;
	}
	
	public void addSong(String Name,double time){
		Songs s1 = new Songs(Name,time);
		sng.add(s1);
	}
	
	public void removeSong(String Name){
		java.util.ListIterator<Songs> songiterator = sng.listIterator(0);
		while(songiterator.hasNext()){
			songiterator.next().getTitle().equalsIgnoreCase(Name);
			songiterator.remove();
		}
	}

	public String getName() {
		return Name;
	}

	public LinkedList<Songs> getSng() {
		return sng;
	}
	
	
}

