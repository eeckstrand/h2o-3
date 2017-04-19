package hex.api;

import hex.mli.loco.LeaveOneCovarOut;
import hex.schemas.LeaveOneCovarOutV3;
import water.H2O;
import water.Job;
import water.api.FramesHandler;
import hex.Model;
import water.api.schemas3.JobV3;
import water.fvec.Frame;
import water.api.Handler;
import water.api.ModelsHandler;
import water.Key;
import water.DKV;

public class LeaveOneCovarOutHandler extends Handler {

    public JobV3 getLoco(int version, final LeaveOneCovarOutV3 args) {

        final Frame frame = FramesHandler.getFromDKV("frame", args.frame.key());
        final Model model= ModelsHandler.getFromDKV("model",args.model.key());
        final String loco_frame_id = args.loco_frame;
        assert model.isSupervised() : "Model " + model._key + " is not supervised.";

        if(loco_frame_id == null){
            args.loco_frame = "loco_"+frame._key.toString() + "_" + model._key.toString();
        }

        final Job<Frame> j = new Job(Key.make(loco_frame_id), Frame.class.getName(), "loco_prediction");

        H2O.H2OCountedCompleter work = new H2O.H2OCountedCompleter() {
                @Override
                public void compute2() {
                    Frame sensitivityAnalysisFrame = LeaveOneCovarOut.leaveOneCovarOut(model,frame);
                    sensitivityAnalysisFrame._key = Key.make(loco_frame_id);
                    DKV.put(sensitivityAnalysisFrame._key,sensitivityAnalysisFrame);
                    tryComplete();
                }
            };

        j.start(work, model._output._names.length);
        return new JobV3().fillFromImpl(j);
    }

}
