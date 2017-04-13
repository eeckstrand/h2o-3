package hex.sensitivityanalysis;

import hex.Model;
import hex.genmodel.utils.DistributionFamily;
import water.exceptions.H2OIllegalArgumentException;
import water.fvec.Frame;
import water.fvec.Vec;
import water.DKV;

public class SensitivityAnalysis {

    public Frame frame;
    public Model model;

    public SensitivityAnalysis(Frame inputFrame, Model inputModel) {

        frame = inputFrame;
        model = inputModel;

    }

    public static Frame SensitivyAnalysisTask(Model m, Frame fr){

        Frame sensitivityAnalysisFrame = new Frame();
        sensitivityAnalysisFrame.add("BasePred",getBasePredictions(m,fr));
        String[] predictors = m._output._names;

        for(int i = 0; i < predictors.length-1; i++){
            sensitivityAnalysisFrame.add("PredDrop_" + predictors[i], getNewPredictions(m,fr,predictors[i]));
        }

        return sensitivityAnalysisFrame;

    }

    public static Vec getBasePredictions(Model m, Frame fr){

        Frame basePredsFr = m.score(fr,null,null,false);

        if(m._parms._distribution == DistributionFamily.gaussian) {
            Vec basePreds = basePredsFr.remove(0);
            basePredsFr.delete();
            return basePreds;
        } else if(m._parms._distribution == DistributionFamily.bernoulli) {
            Vec basePreds = basePredsFr.remove(2);
            basePredsFr.delete();
            return basePreds;
        } else {
            throw new H2OIllegalArgumentException("Sensitivity Analysis is not supported for distribution " + m._parms._distribution);
        }

    }

    public static Vec getNewPredictions(Model m, Frame fr, String colToDrop) {

        Frame workerFrame = new Frame(fr);
        workerFrame.remove(colToDrop);
        DKV.put(workerFrame);
        Frame modifiedPredictionsFr = m.score(workerFrame,null,null,false);
        try {
            if (m._parms._distribution == DistributionFamily.gaussian) {
                Vec modifiedPrediction = modifiedPredictionsFr.remove(0);
                modifiedPredictionsFr.delete();
                return modifiedPrediction;
            } else if (m._parms._distribution == DistributionFamily.bernoulli) {
                Vec modifiedPrediction = modifiedPredictionsFr.remove(2);
                modifiedPredictionsFr.delete();
                return modifiedPrediction;
            } else {
                throw new H2OIllegalArgumentException("Sensitivity Analysis is not supported for distribution " + m._parms._distribution);
            }
        } finally{
            DKV.remove(workerFrame._key);
        }

    }

}


