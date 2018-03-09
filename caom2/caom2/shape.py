# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2016.                            (c) 2016.
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

import math
import six

import numpy as np
from spherical_geometry import polygon
from aenum import Enum

from caom2.caom_util import int_32
from . import caom_util
from . import common

__all__ = ['SegmentType', 'Box', 'Circle', 'Interval', 'Point',
           'Polygon', 'Vertex']


class SegmentType(Enum):
    """
    CLOSE: 0
    LINE: 1
    MOVE: 2
    """
    CLOSE = int_32(0)
    LINE = int_32(1)
    MOVE = int_32(2)


class Box(common.CaomObject):
    def __init__(self, center,
                 width, height):
        """
        Initialize a Box instance
        """
        self.center = center
        self.width = width
        self.height = height

    def get_area(self):
        """ TODO: this is cartesian approximation, use spherical geom? """
        return self._width * self._height

    def get_size(self):
        return math.sqrt(self._width * self._width +
                         self._height * self._height)

    # Properties

    @property
    def center(self):
        """
        type: Point
        """
        return self._center

    @center.setter
    def center(self, value):
        caom_util.type_check(value, Point, 'center', override=False)
        self._center = value

    @property
    def width(self):
        """
        type: float
        """
        return self._width

    @width.setter
    def width(self, value):
        assert value > 0, "width must be positive"
        caom_util.type_check(value, float, 'width', override=False)
        self._width = value

    @property
    def height(self):
        """
        type: float
        """
        return self._height

    @height.setter
    def height(self, value):
        assert value > 0, "height must be positive"
        caom_util.type_check(value, float, 'height', override=False)
        self._height = value


class Circle(common.CaomObject):
    def __init__(self, center,
                 radius):
        """
        Initialize a Circle instance
        """
        self.center = center
        self.radius = radius

    def get_area(self):
        """ TODO: this is cartesian approximation, use spherical geom? """
        return math.pi * self._radius * self._radius

    def get_size(self):
        return 2.0 * self._radius

    # Properties

    @property
    def center(self):
        """
        type: Point
        """
        return self._center

    @center.setter
    def center(self, value):
        caom_util.type_check(value, Point, 'center', override=False)
        self._center = value

    @property
    def radius(self):
        """
        type: float
        """
        return self._radius

    @radius.setter
    def radius(self, value):
        assert value > 0, "radius must be positive"
        caom_util.type_check(value, float, 'radius', override=False)
        self._radius = value


class SubInterval(common.CaomObject):
    def __init__(self, lower, upper):
        self.lower = lower
        self.upper = upper

    # Properties

    @property
    def lower(self):
        """
        type: float
        """
        return self._lower

    @lower.setter
    def lower(self, value):
        caom_util.type_check(value, float, 'lower', override=False)
        has_upper = True
        try:
            self._upper
        except AttributeError:
            has_upper = False
        if has_upper:
            assert not self._upper < value, \
                "SubInterval: attempt to set upper < lower for {}, {}".\
                format(str(value), str(self._lower))
        self._lower = value

    @property
    def upper(self):
        """
        type: float
        """
        return self._upper

    @upper.setter
    def upper(self, value):
        caom_util.type_check(value, float, 'upper', override=False)
        has_lower = True
        try:
            self._lower
        except AttributeError:
            has_lower = False
        if has_lower:
            assert not value < self._lower, \
                "SubInterval: attempt to set upper < lower for {}, {}".\
                format(value, self._lower)
        self._upper = value


class Interval(common.CaomObject):
    def __init__(self, lower, upper, samples=None):

        self.lower = lower
        self.upper = upper
        self.samples = samples
        self.validate()

    def get_width(self):
        return self._upper - self._lower

    @classmethod
    def intersection(cls, i1, i2):
        if i1.lower > i2.upper or i1.upper < i2.lower:
            return None

        lb = max(i1.lower, i2.lower)
        ub = min(i1.upper, i2.upper)
        return cls(lb, ub)

    # Properties

    @property
    def lower(self):
        """
        type: float
        """
        return self._lower

    @lower.setter
    def lower(self, value):
        caom_util.type_check(value, float, 'lower', override=False)
        has_upper = True
        try:
            self._upper
        except AttributeError:
            has_upper = False
        if has_upper:
            assert not self._upper < value, \
                "Interval: attempt to set upper < lower for {], {}".\
                format(self._upper, value)
        self._lower = value

    @property
    def upper(self):
        """
        type: float
        """
        return self._upper

    @upper.setter
    def upper(self, value):
        caom_util.type_check(value, float, 'upper', override=False)
        has_lower = True
        try:
            self._lower
        except AttributeError:
            has_lower = False
        if has_lower:
            assert not value < self._lower, \
                "Interval: attempt to set upper < lower for {}, {}".\
                format(value, self._lower)
        self._upper = value

    @property
    def samples(self):
        """
        type: list
        """
        return self._samples

    @samples.setter
    def samples(self, value):
        if value is not None:
            caom_util.type_check(value, list, 'samples', override=False)
        self._samples = value

    def validate(self):
        """
        Performs a validation of the current object.

        An AssertionError is thrown if the object does not represent an
        Interval
        """
        if self._samples is not None:

            if len(self._samples) == 0:
                raise AssertionError(
                    'invalid interval (samples cannot be empty)')

            prev = None
            for sample in self._samples:
                if sample.lower < self._lower:
                    raise AssertionError(
                        'invalid interval: sample extends below lower bound: '
                        '{} vs {}'.format(sample, self._lower))
                if sample.upper > self._upper:
                    raise AssertionError(
                        'invalid interval: sample extends above upper bound: '
                        '{} vs {}'.format(sample, self._upper))
                if prev is not None:
                    if sample.lower <= prev.upper:
                        raise AssertionError(
                            'invalid interval: sample overlaps previous '
                            'sample:\n{}\nvs\n{}'.format(sample, prev))
                prev = sample


class Point(common.CaomObject):
    def __init__(self, cval1, cval2):
        self.cval1 = cval1
        self.cval2 = cval2

    @property
    def cval1(self):
        """
        type: float
        """
        return self._cval1

    @cval1.setter
    def cval1(self, value):
        caom_util.type_check(value, float, 'cval1', override=False)
        self._cval1 = value

    @property
    def cval2(self):
        """
        type: float
        """
        return self._cval2

    @cval2.setter
    def cval2(self, value):
        caom_util.type_check(value, float, 'cval2', override=False)
        self._cval2 = value


class Polygon(common.CaomObject):
    def __init__(self, points=None, samples=None):
        if points is None:
            self._points = []
        else:
            self._points = points
        self.samples = samples

    # Properties

    @property
    def points(self):
        """
        return: list of points
        """
        return self._points

    @property
    def samples(self):
        """
        return: sample multipolygon associated with this simple polygon
        """
        return self._samples

    @samples.setter
    def samples(self, value):
        if value is not None:
            caom_util.type_check(value, MultiPolygon, 'multipolygon',
                                 override=False)
        self._samples = value


class MultiPolygon(common.CaomObject):
    def __init__(self, vertices=None):
        if vertices is None:
            self._vertices = []
        else:
            self._vertices = vertices

    # Properties

    @property
    def vertices(self):
        """
        type: list of Vertices
        """
        return self._vertices

    def _validate_size_and_end_vertices(self):
        if len(self._vertices) < 4:
            # triangle
            raise AssertionError('invalid polygon: {} vertices (min 4)'.format(
                len(self._vertices)))

        if self._vertices[0].type != SegmentType.MOVE:
            raise AssertionError(
                'invalid polygon: first vertex is not a MOVE vertex')

        if self._vertices[-1].type != SegmentType.CLOSE:
            raise AssertionError(
                'invalid polygon: last vertex is not a CLOSE vertex')

    def validate(self):
        """
        Performs a basic validation of the current object.

        An AssertionError is thrown if the object does not represent a
        multi polygon
        """
        # perform a quick validation of this multipolygon object to fail early
        self._validate_size_and_end_vertices()

        # perform a more detailed validation of this multipolygon object
        mp_validator = self.MultiPolygonValidator()
        for i in range(len(self._vertices)):
            mp_validator.validate(self._vertices[i])


    class MultiPolygonValidator():
        """
        A class to validate the sequencing of vertices in a polygon,
        as well as constructing and validating the polygon.

        An AssertionError is thrown if an incorrect polygon is detected.
        """
        def __init__(self):
            self._lines = 0
            self._open_loop = False
            self._polygon = Polygon()

        def validate(self, vertex):
            if vertex.type == SegmentType.MOVE:
                self.validate_move(vertex)
            elif vertex.type == SegmentType.CLOSE:
                self.validate_close(vertex)
            else:
                self.validate_line(vertex)

        def validate_move(self, vertex):
            if self._open_loop:
                raise AssertionError(
                    'invalid polygon: MOVE vertex when loop open')
            self._lines = 0
            self._open_loop = True
            self._polygon.points.append(Point(vertex.cval1, vertex.cval2))

        def validate_close(self, vertex):
            # close the polygon
            if not self._open_loop:
                raise AssertionError(
                    'invalid polygon: CLOSE vertex when loop close')
            if self._lines < 2:
                raise AssertionError(
                    'invalid polygon: minimum 2 lines required')
            self._open_loop = False
            # SphericalPolygon requires point[0] == point[-1]
            point = self._polygon.points[0]
            self._polygon.points.append(Point(point.cval1, point.cval2))
            # validate the polygons in the multipolygon
            # the following statement should be replaced by: validate_polygon(self._polygon)
            #self._polygon.validate()
            # instantiate a new Polygon for the next iteration
            self._polygon = Polygon()

        def validate_line(self, vertex):
            if not self._open_loop:
                raise AssertionError(
                    'invalid polygon: LINE vertex when loop close')
            self._lines += 1
            self._polygon.points.append(Point(vertex.cval1, vertex.cval2))


class Vertex(Point):
    def __init__(self, cval1, cval2, type):
        super(Vertex, self).__init__(cval1, cval2)
        self.type = type

    # Properties

    @property
    def type(self):
        """
        type: SegmentType
        """
        return self._type

    @type.setter
    def type(self, value):
        caom_util.type_check(value, SegmentType, 'type', override=False)
        self._type = value
