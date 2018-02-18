from pyproj import transform, Proj


def proj_point(point, projIn, projOut):
    projIn = Proj(init=projIn)
    projOut = Proj(init=projOut)

    x, y = transform(projIn, projOut, point['coordinates'][0], point['coordinates'][1])
    point['coordinates'] = [x, y]

    return point
