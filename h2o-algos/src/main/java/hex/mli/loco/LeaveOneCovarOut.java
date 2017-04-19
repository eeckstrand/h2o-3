package hex.mli.loco;

import hex.Model;
import hex.ModelCategory;
import hex.genmodel.utils.DistributionFamily;
import water.MRTask;
import water.ParallelizationTask;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.NewChunk;
import water.fvec.Vec;
import water.DKV;
import water.H2O;
import water.util.Log;

public class LeaveOneCovarOut {

    public static Frame leaveOneCovarOut(Model m, Frame fr){

        Frame locoAnalysisFrame = new Frame();
        if(m._parms._distribution != DistributionFamily.multinomial) {
            locoAnalysisFrame.add("base_pred", getBasepredictions(m, fr)[0]);
        } else {
            locoAnalysisFrame.add(new Frame(getBasepredictions(m, fr)));
            locoAnalysisFrame._names[0] = "base_pred";
        }

        String[] predictors = m._output._names;

        LeaveOneCovariateOutPass[] tasks = new LeaveOneCovariateOutPass[predictors.length-1];

        for(int i = 0; i < tasks.length; i++){
            tasks[i] = new LeaveOneCovariateOutPass(locoAnalysisFrame,fr,m,predictors[i]);
        }

        ParallelizationTask locoCollector = new ParallelizationTask<>(tasks);
        long start = System.currentTimeMillis();
        Log.info("Starting Leave One Covariate Out (LOCO) analysis for model " + m._key + " and frame " + fr._key);
        H2O.submitTask(locoCollector).join();

        if(m._parms._distribution == DistributionFamily.multinomial){
            int[] colsToRemove = new int[locoAnalysisFrame.numCols()-1];
            for(int i =0; i<colsToRemove.length; i++){
                colsToRemove[i] = i+1;
            }
            locoAnalysisFrame.remove(colsToRemove);
        }

        for (int i = 0; i < tasks.length; i++) {
            locoAnalysisFrame.add("rc_" + tasks[i]._predictor, tasks[i]._result[0]);
        }
        Log.info("Finished Leave One Covariate Out (LOCO) analysis for model " + m._key + " and frame " + fr._key +
                " in " + (System.currentTimeMillis()-start)/1000. + " seconds for " + (predictors.length-1) + " columns");

        return locoAnalysisFrame;

    }

    private static class LeaveOneCovariateOutPass extends H2O.H2OCountedCompleter<LeaveOneCovariateOutPass>{

        private final Frame _locoFrame;
        private final Frame _frame;
        private final Model _model;
        private final String _predictor;
        Vec[] _result;

        public LeaveOneCovariateOutPass(Frame locoFrame, Frame fr, Model m, String predictor){
            _locoFrame = locoFrame;
            _frame = fr;
            _model = m;
            _predictor = predictor;
        }

        @Override
        public void compute2() {
            if(_model._parms._distribution == DistributionFamily.multinomial){
                Vec[] predTmp = getNewPredictions(_model,_frame,_predictor);
                _result = new MultiDiffTask(_model._output.nclasses()).doAll(Vec.T_NUM, new Frame().add(_locoFrame).add(new Frame(predTmp))).outputFrame().vecs();
                for (Vec v : predTmp) v.remove();
            } else {
                _result = getNewPredictions(_model, _frame, _predictor);
                new DiffTask().doAll(_locoFrame.vec(0), _result[0]);
            }
            Log.info("Completed Leave One Covariate Out (LOCO) Analysis for column: " + _predictor);
            tryComplete();
        }

    }

    public static Vec[] getBasepredictions(Model m, Frame fr){

        Frame basePredsFr = m.score(fr,null,null,false);

        if(m._parms._distribution == DistributionFamily.bernoulli || m._output.getModelCategory() == ModelCategory.Binomial) {
            Vec basePreds = basePredsFr.remove(2);
            basePredsFr.delete();
            return new Vec[] {basePreds};
        }else if(m._parms._distribution == DistributionFamily.multinomial || m._output.getModelCategory() == ModelCategory.Multinomial){
            return basePredsFr.vecs();
        } else {
            Vec basePreds = basePredsFr.remove(0);
            basePredsFr.delete();
            return new Vec[] {basePreds};
        }

    }

    public static Vec[] getNewPredictions(Model m, Frame fr, String colToDrop) {

        Frame workerFrame = new Frame(fr);
        workerFrame.remove(colToDrop);
        DKV.put(workerFrame);
        Frame modifiedPredictionsFr = m.score(workerFrame,null,null,false);
        try {
            if (m._parms._distribution == DistributionFamily.bernoulli || m._output.getModelCategory() == ModelCategory.Binomial) {
                Vec modifiedPrediction = modifiedPredictionsFr.remove(2);
                modifiedPredictionsFr.delete();
                return new Vec[] {modifiedPrediction};
            } else if(m._parms._distribution == DistributionFamily.multinomial || m._output.getModelCategory() == ModelCategory.Multinomial){
                return modifiedPredictionsFr.vecs();
            } else {
                Vec modifiedPrediction = modifiedPredictionsFr.remove(0);
                modifiedPredictionsFr.delete();
                return new Vec[] {modifiedPrediction};
            }
        } finally{
            DKV.remove(workerFrame._key);
        }

    }

    private static class DiffTask extends MRTask<DiffTask>{
        @Override public void map(Chunk[] c) {
            Chunk _basePred = c[0];
            for(int chnk = 1; chnk < c.length; chnk++){
                for(int row = 0; row < c[0]._len; row++){
                    c[chnk].set(row, c[chnk].atd(row) - _basePred.atd(row));
                }
            }
        }
    }

    private static class MultiDiffTask extends MRTask<MultiDiffTask>{

        private int _numClasses;

        public MultiDiffTask(int numClasses){
            _numClasses = numClasses;
        }

        @Override
        public void map(Chunk[] cs, NewChunk nc) {
            for (int i = 0; i < cs[0]._len; i++) {
                double d = 0;
                for (int j = 1; j < _numClasses; j++) {
                    double val_hat = cs[j + _numClasses].atd(i);
                    double val = cs[j].atd(i);
                    d += val_hat - val;
                }
                nc.addNum(Math.abs(d));
            }
        }
    }
}


