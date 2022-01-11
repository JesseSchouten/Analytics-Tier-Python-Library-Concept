from ADXTable import ADXTableUelPrdAdx

adx_data = ADXTableUelPrdAdx(spark=None)

options = {
  }

print(adx_data.select('playTime', "", options =  options))
