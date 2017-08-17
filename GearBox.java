import java.util.ArrayList;

/**
 * Created by hadoop on 17/8/17.
 */
public class GearBox {

    private int maxGear;
    private int currentGear = 0;
    private ArrayList<Gear> gears;

    public GearBox(int maxGear) {
        this.maxGear = maxGear;
        this.gears = new ArrayList<Gear>();
    }

    public class Gear{
        private int gearNumber;
        private double ratio;

        public Gear(int gearNumber, double ratio) {
            this.gearNumber = gearNumber;
            this.ratio = ratio;
        }

        public double driveSpeed(double revs){
            return this.ratio*revs;
        }
    }
    
}
