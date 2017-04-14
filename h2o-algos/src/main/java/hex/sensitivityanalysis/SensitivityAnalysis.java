package hex.sensitivityanalysis;

import hex.Model;
import hex.genmodel.utils.DistributionFamily;
import water.MRTask;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;
import water.DKV;
import water.H2O;
import water.util.Log;

public class SensitivityAnalysis {

    public static Frame sensitivyAnalysis(Model m, Frame fr){

        Frame sensitivityAnalysisFrame = new Frame();
        sensitivityAnalysisFrame.add("BasePred",getBasePredictions(m,fr));
        String[] predictors = m._output._names;

        SensitivityAnalysisPass[] tasks = new SensitivityAnalysisPass[predictors.length-1];

        for(int i = 0; i < tasks.length; i++){
            tasks[i] = new SensitivityAnalysisPass(sensitivityAnalysisFrame,fr,m,predictors[i]);
        }

        ParallelTasks sensitivityCollector = new ParallelTasks<>(tasks);
        long start = System.currentTimeMillis();
        H2O.submitTask(sensitivityCollector).join();
        for(int i =0; i < tasks.length; i++){
            sensitivityAnalysisFrame.add("PredDrop_" + tasks[i]._predictor,tasks[i]._result);
        }
        Log.info("Sensitivity Analysis finished in " +
                (System.currentTimeMillis()-start)/1000. +
                " seconds for " + predictors.length + " columns");

        return sensitivityAnalysisFrame;

    }

    private static class SensitivityAnalysisPass extends H2O.H2OCountedCompleter<SensitivityAnalysisPass>{

        private final Frame _sensitivityFrame;
        private final Frame _frame;
        private final Model _model;
        private final String _predictor;
        Vec _result;

        public SensitivityAnalysisPass(Frame sensitivityFrame, Frame fr, Model m, String predictor){
            _sensitivityFrame = sensitivityFrame;
            _frame = fr;
            _model = m;
            _predictor = predictor;
        }

        @Override
        public void compute2() {
            _result = getNewPredictions(_model,_frame,_predictor);
            new DiffTask().doAll(_sensitivityFrame.vec(0),_result);
            Log.info("Completed Sensitivity Analysis for column: " + _predictor);
            tryComplete();
        }

    }

    public static Vec getBasePredictions(Model m, Frame fr){

        Frame basePredsFr = m.score(fr,null,null,false);

        if(m._parms._distribution == DistributionFamily.bernoulli) {
            Vec basePreds = basePredsFr.remove(2);
            basePredsFr.delete();
            return basePreds;
        } else if(m._parms._distribution == DistributionFamily.multinomial) {
            Vec basePreds = basePredsFr.remove(0);
            basePredsFr.delete();
            return basePreds;
        } else {
            Vec basePreds = basePredsFr.remove(0);
            basePredsFr.delete();
            return basePreds;
        }

    }

    public static Vec getNewPredictions(Model m, Frame fr, String colToDrop) {

        Frame workerFrame = new Frame(fr);
        workerFrame.remove(colToDrop);
        DKV.put(workerFrame);
        Frame modifiedPredictionsFr = m.score(workerFrame,null,null,false);
        try {
            if (m._parms._distribution == DistributionFamily.bernoulli) {
                Vec modifiedPrediction = modifiedPredictionsFr.remove(2);
                modifiedPredictionsFr.delete();
                return modifiedPrediction;
            } else if (m._parms._distribution == DistributionFamily.multinomial) {
                Vec modifiedPrediction = modifiedPredictionsFr.remove(0);
                modifiedPredictionsFr.delete();
                return modifiedPrediction;
            } else {
                Vec modifiedPrediction = modifiedPredictionsFr.remove(0);
                modifiedPredictionsFr.delete();
                return modifiedPrediction;
            }
        } finally{
            DKV.remove(workerFrame._key);
        }

    }

    private static class DiffTask extends MRTask<DiffTask>{
        @Override public void map(Chunk[] c) {
            Chunk _basePred = c[0]; //First column is always base predictions
            for(int chnk = 1; chnk < c.length; chnk++){
                for(int row = 0; row < c[0]._len; row++){
                    c[chnk].set(row, c[chnk].atd(row) - _basePred.atd(row));
                }
            }
        }
    }

}


