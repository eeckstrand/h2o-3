package hex.api;

import hex.schemas.SensitivityAnalysisV3;
import water.api.FramesHandler;
import hex.Model;
import water.fvec.Frame;
import water.api.Handler;
import water.api.ModelsHandler;
import hex.sensitivityanalysis.SensitivityAnalysis;
import water.Key;
import water.DKV;
import water.api.schemas3.KeyV3;

public class SensitivityAnalysisHandler extends Handler {

    public SensitivityAnalysisV3 getSensitivityAnalysis(int version, SensitivityAnalysisV3 args) {
        Frame frame = FramesHandler.getFromDKV("frame", args.frame.key());
        Model model= ModelsHandler.getFromDKV("model",args.model.key());
        Frame sensitivityAnalysisFrame = SensitivityAnalysis.SensitivyAnalysisTask(model,frame);
        sensitivityAnalysisFrame._key = Key.make("sa" + "_" + frame._key);
        DKV.put(sensitivityAnalysisFrame._key,sensitivityAnalysisFrame);
        args.result = new KeyV3.FrameKeyV3(sensitivityAnalysisFrame._key);
        return args;
    }

}
