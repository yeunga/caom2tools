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
from six.moves.urllib.parse import urlparse
import logging


class ObservationUpdater(object):

    def __init__(self):
        self.archive = None

    def update(self, observation, **kwargs):
        """
        Processes an observation and updates it
        """
        # create cadc data web service client
        if 'subject' in kwargs:
            data_client = CadcDataClient(kwargs['subject'])
        else:
            data_client = CadcDataClient(net.Subject())

        pu = PreviewUpdater(observation, data_client)
        return pu.update()


class PreviewUpdater(object):

    def __init__(self, observation, data_client):
        assert isinstance(observation, Observation), (
            "observation %s is not an Observation".format(observation))
        self.observation = observation
        self.data_client = data_client
        self.archive = self._get_archive()
        assert self.archive, 'Could not determine the associated archive'

    def update(self):
        update = False
        to_delete = {}
        for plane in self.observation.planes.values():
            updated, to_delete = self.update_plane(plane)
            update = update or updated

        return update, to_delete

    def _get_archive(self):
        # returns the associated archive according to the archive used in
        # artifacts
        archive1 = None
        for p in self.observation.planes.values():
            for a in p.artifacts.values():
                archive2 = urlparse(a.uri).path.split('/')[0]
                if archive1 and (archive1 != archive2):
                    raise RuntimeError(('Observation refers to artifact '
                                        'in multiple archives: {}, {}').format(
                        archive1, archive2))
                else:
                    archive1 = archive2
        return archive1

    def _get_meta(self, file):
        meta = None
        try:
            meta = self.data_client.get_file_info(self.archive, file)
        except Exception as e:
            logging.error('file {}/{} not in ad'.format(
                self.archive, file))
            logging.debug(e)
        return meta

    def _update_artifact(self, plane, file, meta, product_type, release_type):
        preview_artifact = Artifact('ad:{}/{}'.format(self.archive, file),
                                    product_type,
                                    release_type)
        preview_artifact.content_length = int(meta['size'])
        preview_artifact.content_type = str(meta['type'])
        plane.artifacts[preview_artifact.uri] = preview_artifact

    def update_cfht_plane(self, plane, root_name):
        update = True
        meta = []
        to_delete = {}
        png_preview_file = '{}_preview_1024.png'.format(root_name)
        png_thumb_file = '{}_preview_256.png'.format(root_name)
        jpg_preview_file = '{}_preview_1024.jpg'.format(root_name)
        jpg_thumb_file = '{}_preview_256.jpg'.format(root_name)
        jpg_zoom_file = '{}_preview_zoom_1024.jpg'.format(root_name)

        png_preview_meta = self._get_meta(png_preview_file)
        png_thumb_meta = self._get_meta(png_thumb_file)
        jpg_preview_meta = self._get_meta(jpg_preview_file)
        jpg_thumb_meta = self._get_meta(jpg_thumb_file)
        jpg_zoom_meta = self._get_meta(jpg_zoom_file)

        if png_preview_meta is not None or png_thumb_meta is not None:
            # at least one png preview file exists
            if png_preview_meta is not None:
                self._update_artifact(plane, png_preview_file, png_preview_meta,
                                      ProductType.PREVIEW, ReleaseType.DATA)
            if png_thumb_meta is not None:
                self._update_artifact(plane, png_thumb_file, png_thumb_meta,
                                      ProductType.THUMBNAIL, ReleaseType.META)
            # mark all jpeg preview files to be deleted
            if jpg_preview_meta is not None:
                meta.append(jpg_preview_meta['name'])
            if jpg_thumb_meta is not None:
                meta.append(jpg_thumb_meta['name'])
            if jpg_zoom_meta is not None:
                meta.append(jpg_zoom_meta['name'])
            if len(meta) > 0:
                to_delete[root_name] = meta
        elif jpg_preview_meta is not None or jpg_thumb_meta is not None or jpg_zoom_meta is not None:
            # no png preview file but at least one jpeg preview file exists
            if jpg_preview_meta is not None:
                self._update_artifact(plane, jpg_preview_file, jpg_preview_meta,
                                      ProductType.PREVIEW, ReleaseType.DATA)
            if jpg_thumb_meta is not None:
                self._update_artifact(plane, jpg_thumb_file, jpg_thumb_meta,
                                      ProductType.THUMBNAIL, ReleaseType.META)
            if jpg_zoom_meta is not None:
                self._update_artifact(plane, jpg_zoom_file, jpg_zoom_meta,
                                      ProductType.PREVIEW, ReleaseType.DATA)
        else:
            return False

        return update, to_delete

    def update_most_plane(self, plane, root_name):
        update = True
        meta = []
        to_delete = {}
        jpg_preview_file = '{}_preview_1024.jpg'.format(root_name)
        jpg_thumb_file = '{}_preview_256.jpg'.format(root_name)

        jpg_preview_meta = self._get_meta(jpg_preview_file)
        jpg_thumb_meta = self._get_meta(jpg_thumb_file)

        if jpg_preview_meta is not None or jpg_thumb_meta is not None:
            if jpg_preview_meta is not None:
                self._update_artifact(plane, jpg_preview_file, jpg_preview_meta,
                                      ProductType.PREVIEW, ReleaseType.DATA)
            if jpg_thumb_meta is not None:
                self._update_artifact(plane, jpg_thumb_file, jpg_thumb_meta,
                                      ProductType.THUMBNAIL, ReleaseType.DATA)
        else:
            return False

        return update, to_delete


    def update_plane(self, plane):
        update = True
        to_delete = {}
        root_name = self._get_root_name(plane)
        if not root_name:
            return update, to_delete

        if self.archive == 'CFHT':
            return self.update_cfht_plane(plane, root_name)

        if self.archive == 'MOST':
            return self.update_most_plane(plane, root_name)

        preview_file = '{}_preview_1024.png'.format(root_name)
        thumb_file = '{}_preview_256.png'.format(root_name)

        # check to see if preview exists
        thumb_meta = None
        preview_meta = None
        try:
            preview_meta = self.data_client.get_file_info(self.archive,
                                                          preview_file)
        except Exception as e:
            logging.error('Preview file {}/{} not in ad'.format(
                self.archive, preview_file))
            logging.debug(e)
        try:
            thumb_meta = self.data_client.get_file_info(self.archive,
                                                        thumb_file)
        except Exception as e:
            logging.error('Thumbnail file {}/{} not in ad'.format(
                self.archive, thumb_file))
            logging.debug(e)

        if not thumb_meta and not preview_meta:
            logging.warning('Nothing to update')
            return False
        else:
            if preview_meta is not None:
                preview_artifact = Artifact('ad:{}/{}'.format(self.archive,
                                                              preview_file),
                                            ProductType.PREVIEW,
                                            ReleaseType.DATA)
                preview_artifact.content_length = int(preview_meta['size'])
                preview_artifact.content_type = str(preview_meta['type'])
                plane.artifacts[preview_artifact.uri] = preview_artifact
            if thumb_meta is not None:
                thumbnail_artifact = Artifact('ad:{}/{}'.format(self.archive,
                                                                thumb_file),
                                              ProductType.THUMBNAIL,
                                              ReleaseType.META)
                thumbnail_artifact.content_length = int(thumb_meta['size'])
                thumbnail_artifact.content_type = str(thumb_meta['type'])
                plane.artifacts[thumbnail_artifact.uri] = thumbnail_artifact

        return update, to_delete

    def _get_root_name(self, plane):
        # By default the root name == observation id
        root = self.observation.observation_id
        if self.archive == 'CGPS':
            # product IDs of planes with individual previews as opposed to
            # planes that share previews with others
            individual_prev = ['1420MHz', '408MHz', 'CO-line', 'HI-line']
            if plane.product_id in individual_prev:
                root = plane.product_id.replace('MHz', '').replace('-line', '')
                root = self.observation.observation_id.replace(
                    '_', '_{}_'.format(root))
                root = root.replace('-', '_')
            elif 'QU' in plane.product_id:
                root = None
        elif self.archive == 'VGPS':
            preview_type = None
            if plane.product_id == '21cm-LineWithCont':
                preview_type = '_hi_continuum_vla'
            elif plane.product_id == '21cm-Line':
                preview_type = '_hi_vla'
            elif plane.product_id == '21cm-Cont':
                preview_type = '_continuum_vla'
            else:
                raise ValueError('Unknown plane product type: {}'.format(
                    plane.product_type))
            root = self.observation.observation_id.replace(
                '_VLA', preview_type)
        return root
