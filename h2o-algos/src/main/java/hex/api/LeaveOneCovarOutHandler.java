package hex.api;

import hex.mli.loco.LeaveOneCovarOut;
import hex.schemas.LeaveOneCovarOutV3;
import water.api.FramesHandler;
import hex.Model;
import water.fvec.Frame;
import water.api.Handler;
import water.api.ModelsHandler;
import water.Key;
import water.DKV;
import water.api.schemas3.KeyV3;

public class LeaveOneCovarOutHandler extends Handler {

    public LeaveOneCovarOutV3 getLoco(int version, LeaveOneCovarOutV3 args) {
        Frame frame = FramesHandler.getFromDKV("frame", args.frame.key());
        Model model= ModelsHandler.getFromDKV("model",args.model.key());
        Frame sensitivityAnalysisFrame = LeaveOneCovarOut.leaveOneCovarOut(model,frame);
        sensitivityAnalysisFrame._key = Key.make("loco" + "_" + frame._key);
        DKV.put(sensitivityAnalysisFrame._key,sensitivityAnalysisFrame);
        args.result = new KeyV3.FrameKeyV3(sensitivityAnalysisFrame._key);
        return args;
    }

}
