# GeoSpatialIndexCode
An implementation to map latitude-longitude pairs to 64 bit integers using QuadTree and Hilbert curve transformation.
We can index on integers and have many datastructures to run range queries on integers in logarithmic time. So mapping the
(latitude-longitude) pairs to integers with preserving their proximity and locality will help index the spatial co-ordinates.

This project is an attempt to write a fast implementation of a GeoSpatialIndex library using the concepts documented in 
"Learning to Rank for Spatiotemporal Search" paper published by FourSquare team. The paper is uploaded into the repository
as 'WSDM2013-final'. I have used the S2-geometry as the base library to tries to index a sphere(assuming earth is a perfect sphere)
recursively using a QuadTree datastructure.

