package com.bigdata.springboot.akka.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;

import java.util.ArrayList;
import java.util.List;

public class UserActor extends AbstractActor {

    List<ActorRef> actorList = new ArrayList<ActorRef>();

    public void Init(){}
	public void CreateActor(){}
	public void StartActor(){}
	public void StopActor(){}
	public void DeleteActor(){}

    @Override
    public Receive createReceive() {
        return null;
    }
}
