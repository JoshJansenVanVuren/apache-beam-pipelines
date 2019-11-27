/*
 * Author: Joshua Jansen Van Vuren
 * Date: 26 Nov 2019
 * Desc: Defines record object
 */

package org.ambrite.josh;

import java.io.Serializable;
import org.ambrite.josh.Constants.ThreeState;

public class Person extends Object implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String name;
    private final String sex;
    private final int age;
    private final int weight;
    private final int height;
    private ThreeState minor;

    // constructor
    public Person(String name, String sex, int age, int weight, int height) {
        this.name = name;
        this.sex = sex;
        this.age = age;
        this.weight = weight;
        this.height = height;
        this.minor = ThreeState.UNSET;
    }

    // another alternative
    public Person(String name, String sex, int age, int weight, int height, ThreeState minor) {
        this.name = name;
        this.sex = sex;
        this.age = age;
        this.weight = weight;
        this.height = height;
        this.minor = minor;
    }

    // getter
    public String getName() {
        return name;
    }

    public String getSex() {
        return sex;
    }

    public int getAge() {
        return age;
    }

    public int getWeight() {
        return weight;
    }

    public int getHeight() {
        return height;
    }

    public ThreeState getIsMinor() {
        return minor;
    }

    @Override
    public boolean equals(final Object o) {
        // If the object is compared with itself then return true
        if (o == this) {
            return true;
        }

        /*
         * Check if o is an instance of Person or not "null instanceof [type]" also
         * returns false
         */
        if (!(o instanceof Person)) {
            return false;
        }

        // typecast o to Person so that we can compare data members
        final Person c = (Person) o;

        // Compare the data members and return accordingly
        boolean members = true;

        if (!c.name.equals(this.name))
            members = false;
        if (!c.sex.equals(this.sex))
            members = false;
        if (!(c.age == this.age))
            members = false;
        if (!(c.weight == this.weight))
            members = false;
        if (!(c.height == this.height))
            members = false;
        if (!(c.minor == this.minor))
            members = false;

        return members;
    }

    @Override
    public String toString() {
        String output = this.name + ", " + this.sex + ", " + this.age + ", " + this.weight + ", " + this.height;

        if (minor != ThreeState.UNSET) {
            if (minor == ThreeState.TRUE) {
                output = output + ", true";
            } else {
                output = output + ", false";
            }
        }

        return output;
    }

    public String toNiceString() {
        String output = "\nName: " + this.name + "\tSex: " + this.sex + "\tAge: " + this.age + "\tWeight: "
                + this.weight + "\tHeight: " + this.height;

        if (minor != ThreeState.UNSET) {
            if (minor == ThreeState.TRUE) {
                output = output + "\tMinor: true";
            } else {
                output = output + "\tMinor: false\n";
            }
        }

        return output;
    }
}