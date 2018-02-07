package com.github.peterungvari.javasparkdatasets;

public class PersonItemCount {

    private Person person;
    private int itemCount;

    public PersonItemCount() {
    }

    public PersonItemCount(Person person, int itemCount) {
        this.person = person;
        this.itemCount = itemCount;
    }

    public Person getPerson() {
        return person;
    }

    public void setPerson(Person person) {
        this.person = person;
    }

    public int getItemCount() {
        return itemCount;
    }

    public void setItemCount(int itemCount) {
        this.itemCount = itemCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PersonItemCount that = (PersonItemCount) o;

        if (itemCount != that.itemCount) return false;
        return person != null ? person.equals(that.person) : that.person == null;
    }

    @Override
    public int hashCode() {
        int result = person != null ? person.hashCode() : 0;
        result = 31 * result + itemCount;
        return result;
    }
}
