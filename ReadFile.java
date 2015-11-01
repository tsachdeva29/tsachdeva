package com.scalawagstudio.emailDirect;

import java.util.ArrayList;

class ReadFile{
    Integer id;
    String name;
    public ReadFile(Integer id, String name){
        this.id=id;
        this.name=name;
    }
        
    public Integer getID(){
        return id;
    }
    
    public String getName(){
        return name;
    }
    public String toString(){
    	return this.getID()+this.getName();
    }
}