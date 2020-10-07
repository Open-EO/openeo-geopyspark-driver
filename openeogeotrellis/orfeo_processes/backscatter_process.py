
def calibration(s1_grd_array):
    """
    A first version of a function that calibrates Sentinel-1 data.

    @param s1_grd_array:
    @return:
    """
    #import sys
    #sys.path.append('/home/driesj/OTB-7.2.0-Linux64/lib/python')
    import otbApplication as otb

    #zip files do not work, has to be extracted
    test_file = 's1data/S1B_IW_GRDH_1SDV_20201004T060621_20201004T060646_023659_02CF3D_593D.SAFE/measurement/s1b-iw-grd-vh-20201004t060621-20201004t060646-023659-02cf3d-002.tiff'


    extractROI = otb.Registry.CreateApplication("ExtractROI")

    offset = 2024
    extractROI.SetParameterString("in", test_file)
    extractROI.SetParameterString("mode","extent")
    extractROI.SetParameterFloat("mode.extent.ulx", offset)
    extractROI.SetParameterFloat("mode.extent.uly", offset)
    extractROI.SetParameterFloat("mode.extent.lrx", offset+256)
    extractROI.SetParameterFloat("mode.extent.lry", offset+256)

    extractROI.Execute()
    SARCalibration = otb.Registry.CreateApplication('SARCalibration')
    #parameter keys for sar calibration: ('in', 'out', 'noise', 'lut', 'ram')
    SARCalibration.SetParameterInputImage("in",extractROI.GetParameterOutputImage("out"))

    SARCalibration.SetParameterValue('noise',True)

    #this execut does not actually compute the result
    SARCalibration.Execute()

    SARCalibration.GetImageAsNumpyArray('out')