library(R.utils)
library(curl)
library(raster)
library(parallel)
library(stringr)
library(RCurl)
#install.packages("rlang")

# Establece los directorios de destino
directorio_salidas2 <- "/var/database_CST/Persian/diario/tif_0.25"
directorio_salidas <- "/var/database_CST/Persian/diario/tif_0.04"
directorio_temporal <- "/var/database_CST/Persian/diario/Temp_files"

# Crea el directorio de destino si no existe
if (!dir.exists(directorio_salidas)) dir.create(directorio_salidas, recursive = TRUE)
if (!dir.exists(directorio_salidas2)) dir.create(directorio_salidas2, recursive = TRUE)
if (!dir.exists(directorio_temporal)) dir.create(directorio_temporal, recursive = TRUE)

# Define la ruta completa de las salidas
ruta_salida <- file.path(directorio_temporal, "ok_test.grd")
ruta_salida2 <- file.path(directorio_temporal, "ok_test2.grd")

# generar mascara 0.04
r <- raster(xmn=0, xmx=360, ymn=-60, ymx=60, nrow=3000, ncol=9000, crs="+proj=longlat +datum=WGS84")
values(r) <- 1
r <- writeRaster(r, ruta_salida, datatype="FLT4S")
x <- readLines(ruta_salida)
x[grep("byteorder", x)] <- "byteorder=big"
x[grep("nodatavalue", x)] <- "nodatavalue=-9999"
writeLines(x, ruta_salida)

# generar mascara 0.25
r <- raster(xmn=0, xmx=360, ymn=-60, ymx=60, nrow=480 , ncol=1440 , crs="+proj=longlat +datum=WGS84")
values(r) <- 1
r <- writeRaster(r, ruta_salida2, datatype="FLT4S")
x <- readLines(ruta_salida2)
x[grep("byteorder", x)] <- "byteorder=little"
x[grep("nodatavalue", x)] <- "nodatavalue=-9999"
writeLines(x, ruta_salida2)


generarUrls <- function(fecha_inicial_pagina1,fecha_final_pagina1,fecha_inicial_pagina2,fecha_final_pagina2) {
  # URLs de las páginas
  pagina1 <- "ftp://persiann.eng.uci.edu/CHRSdata/PERSIANN-CCS/daily/"
  pagina2 <- "ftp://persiann.eng.uci.edu/CHRSdata/PERSIANN-CDR/daily/"
  
  
  # Listas para almacenar las URLs
  urls <- list()
  urls2 <- list()
  
  rango1 <- seq.Date(fecha_inicial_pagina1, fecha_final_pagina1, by = "day")
  rango2 <- seq.Date(fecha_inicial_pagina2, fecha_final_pagina2, by = "day")
  
  # Iterar sobre el rango de fechas y construir las URLs
  for (fecha in as.character(rango1)) {
    yyddd <- format(as.Date(fecha), "%y%j")
    filename <- paste0("aB1_d", yyddd, ".bin.gz")
    url <- paste0(pagina2, filename)
    urls2 <- c(urls2, url)
    
  }
  
  for (fecha in as.character(rango2)) {
    yyddd <- format(as.Date(fecha), "%y%j")
    filename <- paste0("rgccs1d", yyddd, ".bin.gz")
    url <- paste0(pagina1, filename)
    urls <- c(urls, url)
  }
  
  
  return(list(lista1 = urls, lista2 = urls2))
}

# Define la función para procesar cada archivo
procesar_archivo <- function(url, directorio_temporal, directorio_salidas,ruta_salida) {
  gzf <- file.path(directorio_temporal, basename(url))
  print(gzf)
  if (url.exists(url)) {
    curl_download(url, gzf)
    R.utils::gunzip(gzf)
    f <- gsub("\\.gz$", "", gzf)
    file.rename(f, extension(f, "gri"))
    fg <- extension(f, "grd")
    file.copy(ruta_salida, fg)
    # Limitar los valores a un decimal
    r <- raster(fg) * 1
    r <- rotate(r)
    # Extraer la fecha del nombre del archivo
    yyddd <- str_extract(basename(url), "(\\d{5})")
    fecha <- as.Date(yyddd, format = "%y%j")
    
    # Construir el nombre de salida en el formato deseado
    nombre_salida <- file.path(
      directorio_salidas,
      paste0("persiann.", format(fecha, "%Y.%m.%d"), ".tif")
    )
    # Definir la extensión para Sudamérica (ajusta las coordenadas según tu necesidad)
    extension_sudamerica <- extent(-90, -35, -60, 15)
    
    # Recortar el RasterStack
    r <- crop(r, extension_sudamerica)
    writeRaster(r, nombre_salida, format = "GTiff", overwrite = TRUE)
    # Elimina el archivo temporal .gri
    file.remove(extension(f, "gri"))
    file.remove(extension(fg, "grd"))
    return(nombre_salida)
  }
}


#definir fechas requeridas
fecha_inicial_pagina1 <- as.Date("1983-01-01")
fecha_final_pagina1 <- as.Date("1983-01-05") #llega hasta el 2002-12-31, ya que 2003-01-01 esta en 0.04 grados
fecha_inicial_pagina2 <- as.Date("2003-01-01")
fecha_final_pagina2 <- as.Date("2003-01-05") #Sys.Date()


resultados <- generarUrls(fecha_inicial_pagina1,fecha_final_pagina1,fecha_inicial_pagina2,fecha_final_pagina2)
urls <- resultados$lista1
urls2 <- resultados$lista2


# Establece el número de núcleos a utilizar (ajústalo según tu máquina)
num_cores <- 4 
#running it with mclapply 
ptm_mc <- Sys.time()
# Procesa las URLs en paralelo
resultados <- mclapply(1:length(urls), function(i) {
  nombre <- procesar_archivo(urls[[i]], directorio_temporal, directorio_salidas, ruta_salida)
  
}, mc.cores = num_cores, mc.preschedule = FALSE)

time_mclapply <- Sys.time() - ptm_mc
print(time_mclapply)


# Procesa las URLs en paralelo
ptm_mc <- Sys.time()
resultados2 <- mclapply(1:length(urls2), function(i) {
  nombre <- procesar_archivo(urls2[[i]], directorio_temporal, directorio_salidas2, ruta_salida2)
  
}, mc.cores = num_cores, mc.preschedule = FALSE)

time_mclapply <- Sys.time() - ptm_mc
print(time_mclapply)



# # Función para apilar y recortar
# apilar_y_recortar <- function(lista_tifs, directorio_salida, archivo_salida) {
#   # Leer el primer raster para obtener las propiedades
#   primer_raster <- raster(lista_tifs[[1]])
#   
#   # Crear una lista para almacenar los rasters
#   raster_list <- list()
#   
#   # Iterar sobre cada archivo TIF
#   for (i in seq(1, length(lista_tifs))) {
#     # Leer el raster
#     r <- raster(lista_tifs[[i]])
#     
#     # Almacenar el raster en la lista
#     raster_list[[length(raster_list) + 1]] <- r
#   }
#   
#   # Apilar los rasters en un objeto RasterStack
#   raster_stack <- stack(raster_list)
#   
#   
#   # Escribir el RasterStack recortado a un archivo TIF
#   writeRaster(raster_stack, file.path(directorio_salida, archivo_salida), format = "GTiff", overwrite = TRUE)
# }
# 
# 
# tifs <- resultados
# tifs2 <- resultados2
# 
# 
# archivo_salida <- paste0("persiann_stacked.",fecha_inicial_pagina2,"-",fecha_final_pagina2,".tif")
# archivo_salida2 <- paste0("persiann_stacked.",fecha_inicial_pagina1,"-",fecha_final_pagina1,".tif")
# # Llamar a la función
# apilar_y_recortar(tifs, directorio_salidas, archivo_salida)
# # Llamar a la función
# apilar_y_recortar(tifs2, directorio_salidas, archivo_salida2)

