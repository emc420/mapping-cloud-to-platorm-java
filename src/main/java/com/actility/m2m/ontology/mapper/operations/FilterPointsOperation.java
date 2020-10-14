package com.actility.m2m.ontology.mapper.operations;

import com.actility.m2m.flow.data.DownMessage;
import com.actility.m2m.flow.data.Point;
import com.actility.m2m.flow.data.UpMessage;
import com.actility.m2m.ontology.mapper.OperationHandler;
import com.actility.m2m.ontology.mapping.java.lib.data.*;

import java.util.*;

public class FilterPointsOperation implements OperationHandler {

    @Override
    public Optional<UpMessage> applyUpOperation(UpMessage message, UpOperation upOperation) {
        List<String> points;
        UpFilterPointsOperation upFilterPointsOperation = (UpFilterPointsOperation) upOperation;
        Map<String, Point> pointsUpMessage = message.points;
        if(pointsUpMessage!=null){
            points = upFilterPointsOperation.points;
            Map<String, Point> newPoints = new HashMap<>();
            if(!points.isEmpty()){
                for (Map.Entry<String, Point> entry : pointsUpMessage.entrySet()) {
                    if(points.contains(entry.getKey())){
                        newPoints.put(entry.getKey(), entry.getValue());
                    }
                }
            }
            return Optional.of(UpMessage.newUpMessageBuilder(message).points(newPoints).build());
        }
        return Optional.of(message);
    }

    @Override
    public Optional<DownMessage> applyDownOperation(DownMessage message, DownOperation upOperation) {
        return Optional.empty();
    }
}
