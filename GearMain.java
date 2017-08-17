/**
 * Created by hadoop on 17/8/17.
 */
public class GearMain {
    static GearBox maruthi = new GearBox(5);
    static GearBox.Gear first = maruthi.new Gear(1,200);
    public static void main (String[] args){
        System.out.println("The Speed of Maruthi at First Gear is "+ first.driveSpeed(100));
    }
}
