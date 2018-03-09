# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2018.                            (c) 2018.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#  $Revision: 4 $
#
# ***********************************************************************
#

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import pytest

from caom2 import shape
from caom2utils.polygonvalidator import validate_multipolygon, validate_polygon


def test_open_polygon():
    p1 = shape.Point(-117.246094, 52.942018)
    p2 = shape.Point(-101.601563, 56.535258)
    p3 = shape.Point(-97.382813, 44.809122)
    p4 = shape.Point(-111.445313, 37.405074)
    # SphericalPolygon requires p1 == p5 for a closed polygon
    p5 = shape.Point(-117.246094, 52.942018)
    too_few_points = [p1, p2]
    min_closed_points = [p1, p2, p3]
    closed_points = [p1, p2, p3, p4, p5]
    counter_clockwise_points = [p4, p3, p2, p1]

    # should detect that the polygons is not clockwise
    with pytest.raises(AssertionError) as ex:
        validate_polygon(shape.Polygon(counter_clockwise_points))
    assert('not in clockwise direction' in str(ex.value))
    # should detect that polygon is requires a minimum of 4 points
    with pytest.raises(AssertionError) as ex:
        validate_polygon(shape.Polygon(too_few_points))
    assert('invalid polygon: 2 points' in str(ex.value))

    # polygon default constructor
    validate_polygon(shape.Polygon())

    # should detect that polygon is closed
    validate_polygon(shape.Polygon(min_closed_points))
    validate_polygon(shape.Polygon(closed_points))

    # should detect that multipolygon is not closed
    v0 = shape.Vertex(-126.210938, 67.991108, shape.SegmentType.MOVE)
    v1 = shape.Vertex(-108.984375, 70.480896, shape.SegmentType.LINE)
    v2 = shape.Vertex(-98.789063, 66.912834, shape.SegmentType.LINE)
    v3 = shape.Vertex(-75.234375, 60.217991, shape.SegmentType.LINE)
    v4 = shape.Vertex(-87.890625, 52.241256, shape.SegmentType.LINE)
    v5 = shape.Vertex(-110.742188, 54.136696, shape.SegmentType.LINE)
    v6 = shape.Vertex(0.0, 0.0, shape.SegmentType.CLOSE)
    v7 = shape.Vertex(24.609375, 62.895218, shape.SegmentType.MOVE)
    v8 = shape.Vertex(43.593750, 67.322924, shape.SegmentType.LINE)
    v9 = shape.Vertex(55.898438, 62.734601, shape.SegmentType.LINE)
    v10 = shape.Vertex(46.757813, 56.145550, shape.SegmentType.LINE)
    v11 = shape.Vertex(26.015625, 55.354135, shape.SegmentType.LINE)
    v12 = shape.Vertex(0.0, 0.0, shape.SegmentType.CLOSE)
    closed_vertices = [
        v0, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12]

    # should detect that multipolygon is closed
    # TODO: uncomment after merge and validate_multipolygon is available
    validate_multipolygon(shape.MultiPolygon(closed_vertices))


def test_polygon_self_intersection():
    # should detect self segment intersection of the polygon not near a
    # Pole
    p1 = shape.Point(-115.488281, 45.867063)
    p2 = shape.Point(-91.230469, 36.075742)
    p3 = shape.Point(-95.800781, 54.807017)
    p4 = shape.Point(-108.457031, 39.951859)
    p5 = shape.Point(0.0, 0.0)
    points_with_self_intersecting_segments = [p1, p2, p3, p4, p5]
    with pytest.raises(AssertionError) as ex:
        poly = shape.Polygon(points_with_self_intersecting_segments)
        validate_polygon(poly)
    assert('self intersecting' in str(ex.value))

    # should detect self segment intersection of the polygon near the
    # South Pole, with the Pole outside the polygon
    p1 = shape.Point(0.6128286003, -89.8967940441)
    p2 = shape.Point(210.6391743183, -89.9073892376)
    p3 = shape.Point(90.6405151921, -89.8972874698)
    p4 = shape.Point(270.6114701911, -89.90689353)
    p5 = shape.Point(0.0, 0.0)
    points_with_self_intersecting_segments = [p1, p2, p3, p4, p5]
    with pytest.raises(AssertionError) as ex:
        poly = shape.Polygon(points_with_self_intersecting_segments)
        validate_polygon(poly)
    assert('self intersecting' in str(ex.value))

    # should detect self segment intersection of the polygon near the
    # South Pole, with the Pole inside the polygon
    p1 = shape.Point(0.6128286003, -89.8967940441)
    p2 = shape.Point(130.6391743183, -89.9073892376)
    p3 = shape.Point(90.6405151921, -89.8972874698)
    p4 = shape.Point(270.6114701911, -89.90689353)
    p5 = shape.Point(0.0, 0.0)
    points_with_self_intersecting_segments = [p1, p2, p3, p4, p5]
    with pytest.raises(AssertionError) as ex:
        poly = shape.Polygon(points_with_self_intersecting_segments)
        validate_polygon(poly)
    assert('self intersecting' in str(ex.value))

    # should detect self segment intersection of the polygon which
    # intersects with meridian = 0
    p1 = shape.Point(-7.910156, 13.293411)
    p2 = shape.Point(4.042969, 7.068185)
    p3 = shape.Point(4.746094, 18.030975)
    p4 = shape.Point(-6.855469, 6.369894)
    p5 = shape.Point(0.0, 0.0)
    points_with_self_intersecting_segments = [p1, p2, p3, p4, p5]
    with pytest.raises(AssertionError) as ex:
        poly = shape.Polygon(points_with_self_intersecting_segments)
        validate_polygon(poly)
    assert('self intersecting' in str(ex.value))
