#!/usr/bin/env python2.7
# # -*- coding: utf-8 -*-
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

from caom2 import Observation, ProductType, ReleaseType, Artifact
from cadcdata import CadcDataClient
from cadcutils import net
from builtins import str


class ObservationUpdater(object):
    """Plugin that does not update the observation"""

    def __init__(self):
        self.collection = None

    def update(self, observation, **kwargs):
        """
        Processes an observation and updates it
        """
        assert isinstance(observation, Observation), (
            "observation %s is not an Observation".format(observation))

        # TODO Remove this. Archive should be dedeced from the uris of
        # other artifacts in the plane
        if 'collection' not in kwargs:
            raise ValueError('caom2-repo version too old. Please upgrade')

        self.collection = kwargs['collection']

        if self.collection not in ['CGPS']:
            raise ValueError('{} collection not supported'.format(
                self.collection))

        # create cadc data web service client
        if 'subject' in kwargs:
            client = CadcDataClient(kwargs['subject'])
        else:
            client = CadcDataClient(net.Subject())

        for plane in observation.planes.values():
            self.update_cgps_preview(observation.observation_id, plane, client)



    def update_cgps_preview(self, observation_id, plane, client):

        # check to see if preview exists
        preview_file = '{}_preview_1024.png'.format(observation_id.upper())
        thumb_file = '{}_preview_256.png'.format(observation_id.upper())
        try:
            preview_meta = client.get_file_info(self.collection,
                                                preview_file)
        except:
            print('File {}/{} not in ad'.format(self.collection,
                                                preview_file))
        try:
            thumb_meta = client.get_file_info(self.collection,
                                                thumb_file)
        except:
            print('File {}/{} not in ad'.format(self.collection,
                                                thumb_file))

        if not thumb_meta and not preview_meta:
            print('Nothing to do. Returning')
            return

        for artifact in plane.artifacts.values():
            if (artifact.product_type == ProductType.PREVIEW) or \
               (artifact.product_type == ProductType.THUMBNAIL):
                del plane.artifacts[artifact.uri]

        preview_artifact = Artifact('ad:{}/{}'.format(self.collection,
                                                      preview_file),
                                    ProductType.PREVIEW,
                                    ReleaseType.DATA)
        thumbnail_artifact = Artifact('ad:{}/{}'.format(self.collection,
                                                        thumb_file),
                                      ProductType.THUMBNAIL,
                                      ReleaseType.META)

        plane.artifacts[preview_artifact.uri] = preview_artifact
        plane.artifacts[thumbnail_artifact.uri] = thumbnail_artifact

