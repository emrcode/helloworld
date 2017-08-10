package sample_project;

import java.util.*;

import javassist.bytecode.Descriptor.Iterator;

public class PlayList {
	private String playlistname;
	private LinkedList<Songs> songslist;
	private ArrayList<Albums> existingAlbumList;
	
	public PlayList(String playlistname, 
			ArrayList<Albums> existingAlbumList) {
		this.playlistname = playlistname;
		this.songslist = null;
		this.existingAlbumList = existingAlbumList;
	}

	public void addsongtoplaylist(String albumName,String songName){
		java.util.Iterator<Albums> it = existingAlbumList.iterator();
		while(it.hasNext()){
			Albums currentAlbum = it.next(); 
			if(currentAlbum.getName().equalsIgnoreCase(albumName)){
				LinkedList<Songs> currentAlbumsSongList = currentAlbum.getSng();
				ListIterator<Songs> itsng = currentAlbumsSongList.listIterator();
				while(itsng.hasNext()){
					Songs currentAlbumSong = itsng.next();
					if(currentAlbumSong.getTitle().equalsIgnoreCase(songName)){
						songslist.add(currentAlbumSong);
					}
					else
						System.out.println("No Such Song found in Album");
				}
			}
			else
				System.out.println("No Such Album Found");
		}
	}
	
	public void listSongs(){
		java.util.Iterator<Songs> it = songslist.iterator();
		while(it.hasNext()){
			System.out.println("Song Name" + it.next().getTitle() + "Play Time" + it.next().getTime());
		}
		
	}
	

}
