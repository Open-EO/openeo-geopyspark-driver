
def calibration(s1_grd_array):
    """
    A first version of a function that calibrates Sentinel-1 data.

    @param s1_grd_array:
    @return:
    """
    import sys
    sys.path.append('/home/driesj/OTB-7.2.0-Linux64/lib/python')
    import otbApplication as otb

    #zip files do not work, has to be extracted
    #ref file:
    #/data/MTDA/CGS_S1/CGS_S1_GRD_SIGMA0_L1/2020/10/04/S1B_IW_GRDH_SIGMA0_DV_20201004T060621_DESCENDING_8_593D_V110/S1B_IW_GRDH_SIGMA0_DV_20201004T060621_DESCENDING_8_593D_V110_VH.tif
    test_file = 's1data/S1B_IW_GRDH_1SDV_20201004T060621_20201004T060646_023659_02CF3D_593D.SAFE/measurement/s1b-iw-grd-vh-20201004t060621-20201004t060646-023659-02cf3d-002.tiff'


    extractROI = otb.Registry.CreateApplication("ExtractROI")

    offset = 2024
    extractROI.SetParameterString("in", test_file)
    extractROI.SetParameterString("mode","extent")
    extractROI.SetParameterString("mode.extent.unit","lonlat")
    (ulx,uly,lrx,lry)=2.856445,51.155878,3.731232,51.442881

    #(ulx, uly, lrx, lry) = offset, offset, offset+256, offset+256
    extractROI.SetParameterFloat("mode.extent.ulx", ulx)
    extractROI.SetParameterFloat("mode.extent.uly", uly)
    extractROI.SetParameterFloat("mode.extent.lrx", lrx)
    extractROI.SetParameterFloat("mode.extent.lry", lry)

    extractROI.Execute()

    SARCalibration = otb.Registry.CreateApplication('SARCalibration')
    #parameter keys for sar calibration: ('in', 'out', 'noise', 'lut', 'ram')

    patch=True
    if patch:
        SARCalibration.SetParameterInputImage("in",extractROI.GetParameterOutputImage("out"))
    else:
        SARCalibration.SetParameterString("in", test_file)

    SARCalibration.SetParameterValue('noise',True)
    SARCalibration.SetParameterInt('ram',512)

    #this execut does not actually compute the result
    SARCalibration.Execute()

    OrthoRect = otb.Registry.CreateApplication('OrthoRectification')

    OrthoRect.SetParameterInputImage("io.in", SARCalibration.GetParameterOutputImage("out"))

    OrthoRect.SetParameterString("elev.dem","/home/driesj/dems")
    OrthoRect.SetParameterString("elev.geoid", "/home/driesj/egm96.grd")
    OrthoRect.SetParameterValue("map.utm.northhem",True)
    OrthoRect.SetParameterInt("map.epsg.code",32631)

    OrthoRect.SetParameterFloat("outputs.spacingx",10.0)
    OrthoRect.SetParameterFloat("outputs.spacingy", -10.0)
    OrthoRect.SetParameterString("interpolator","nn")
    OrthoRect.SetParameterFloat("opt.gridspacing",40.0)
    OrthoRect.SetParameterInt("opt.ram",512)

    OrthoRect.Execute()
    OrthoRect.SetParameterString("io.out","sigma0.tif")
    OrthoRect.ExecuteAndWriteOutput()
    #OrthoRect.GetImageAsNumpyArray('io.out')
