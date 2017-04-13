package hex.schemas;

import water.Iced;
import water.api.API;
import water.api.schemas3.KeyV3;
import water.api.schemas3.SchemaV3;
import water.api.schemas3.KeyV3.FrameKeyV3;

public class SensitivityAnalysisV3 extends SchemaV3<Iced, SensitivityAnalysisV3> {

    @API(help="supervised model", required = true, direction = API.Direction.INPUT)
    public KeyV3.ModelKeyV3 model;

    @API(help="input frame",required = true, direction = API.Direction.INPUT)
    public KeyV3.FrameKeyV3 frame;

    @API(help="output frame", direction=API.Direction.OUTPUT)
    public FrameKeyV3 result;

}
