# # -*- coding: utf-8 -*-
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

import argparse
import imp
import logging
import os, errno
import os.path
import sys

from cadcutils import net
from cadcutils import util
from caom2repo import CAOM2RepoClient

# from . import version as caom2repo_version
from caom2repo import version

CFHT_DELIMITER = '_preview'
BATCH_SIZE = 1000
JUNK_REPORT = 'files_with_no_recognizable_observationID.txt'
NO_OBSERVATION_REPORT = 'files_with_no_associated_observation.txt'
UPDATED_REPORT = 'files_with_observation_updated.txt'
FAILED_REPORT = 'files_with_failed_observation_update.txt'
SKIPPED_REPORT = 'files_with_observation_update_skipped.txt'
TO_DELETE_REPORT = 'files_to_be_deleted_for_observation.txt'
REPORTS = [JUNK_REPORT, NO_OBSERVATION_REPORT, UPDATED_REPORT,
           FAILED_REPORT, SKIPPED_REPORT, TO_DELETE_REPORT]

CAOM2REPO_OBS_CAPABILITY_ID =\
    'vos://cadc.nrc.ca~vospace/CADC/std/CAOM2Repository#obs-1.1'

# resource ID for info
DEFAULT_RESOURCE_ID = 'ivo://cadc.nrc.ca/caom2repo'
APP_NAME = 'caom2repo'


class CAOM2FixPreviewClient(object):
    """Class to do CRUD + visitor actions on a CAOM2 collection repo."""

    def __init__(self, subject, logLevel=logging.INFO,
                 resource_id=DEFAULT_RESOURCE_ID, host=None):
        """
        Instance of a CAOM2FixPreviewClient
        :param subject: the subject performing the action
        :type cadcutils.auth.Subject
        :param server: Host server for the caom2repo service
        """
        self.level = logLevel
        logging.basicConfig(
            format='%(asctime)s %(process)d %(levelname)-8s %(name)-12s ' +
                   '%(funcName)s %(message)s',
            level=logLevel, stream=sys.stdout)
        self.logger = logging.getLogger('CAOM2FixPreviewClient')
        self.resource_id = resource_id
        self.host = host
        self._subject = subject
        self.agent = "cfht_fixPreview"

    def clear_reports(self):
        for report in REPORTS:
            with open(report, 'w') as the_file:
                the_file.write("")

    def write_report(self, name, data):
        with open(name, 'a') as the_file:
            for key, values in data.items():
                for v in values:
                    the_file.write(key + ' ' + v)

    def visit(self, plugin, obs_file):
        """
        Main processing function that iterates through the observations of
        the collection and updates them according to the algorithm
        of the plugin function
        :param plugin: path to python file that contains the algorithm to be
                applied to visited observations
        :param obs_file: path to file that contains the preview files to be fixed
        """
        no_observation = {}
        updated = {}
        failed = {}
        skipped = {}
        to_delete = {}

        assert obs_file is not None
        if not os.path.isfile(plugin):
            raise Exception('Cannot find plugin file ' + plugin)

        collection, theRest = obs_file.name.split('_', 1)
        assert collection is not None

        self._load_plugin_class(plugin)

        observations = {}
        if obs_file is not None:
            # get observation IDs from file, no batching
            observations = self._get_obs_from_file(obs_file, CFHT_DELIMITER)

        keys = observations.keys()
        start = 0
        stop = BATCH_SIZE
        remaining_size = len(keys) - start
        self.clear_reports()
        while remaining_size > 0:
            current_keys = keys[start:stop]
            results = [
                self._process_observation_id(collection, k, observations[k])
                for k in current_keys]
            for n, u, f, s, d in results:
                if n:
                    no_observation.update(n)
                if u:
                    updated.update(f)
                if f:
                    failed.update(f)
                if s:
                    skipped.update(s)
                if d:
                    to_delete.update(d)

            if no_observation:
                self.write_report(NO_OBSERVATION_REPORT, no_observation)
                no_observation = {}
            if updated:
                self.write_report(UPDATED_REPORT, updated)
                updated = {}
            if failed:
                self.write_report(FAILED_REPORT, failed)
                failed = {}
            if skipped:
                self.write_report(SKIPPED_REPORT, skipped)
                skipped = {}
            if to_delete:
                self.write_report(TO_DELETE_REPORT, to_delete)
                to_delete = {}

            start = start + BATCH_SIZE
            remaining_size = len(keys) - start
            if BATCH_SIZE > remaining_size:
                stop = stop + remaining_size
            else:
                stop = stop + BATCH_SIZE

    def _update_observation(self, client, collection, observation, observationID, filenames):
        to_delete = {}
        updated = {}
        skipped = {}
        failed = {}
        try:
            update, to_delete = self.plugin.update(observation=observation,
                                                   subject=self._subject,
                                                   collection=collection)
            if update is False:
                self.logger.info('SKIP {}'.format(observation.observation_id))
                skipped[observation.observation_id] = filenames
            else:
                client.post_observation(observation)
                self.logger.debug(
                    'UPDATED {}'.format(observation.observation_id))
                updated[observation.observation_id] = filenames
        except TypeError as e:
            if "unexpected keyword argument" in str(e):
                raise RuntimeError(
                    "{} - To fix the problem, please add the **kwargs "
                    "argument to the list of arguments for the update"
                    " method of your plugin.".format(str(e)))
            else:
                # other unexpected TypeError
                raise e
        except Exception as e:
            failed[observationID] = filenames
            self.logger.error(
                'FAILED {} - Reason: {}'.format(observationID, e))
        return updated, skipped, failed, to_delete

    def _process_observation_id(self, collection, observationID, filenames):
        observation = None
        no_observation = {}
        updated = {}
        skipped = {}
        failed = {}
        to_delete = {}
        self.logger.info('Process observation: ' + observationID)
        client = CAOM2RepoClient(self._subject, self.level, self.resource_id,
                                 self.host)
        try:
            observation = client.get_observation(collection, observationID)
            updated, skipped, failed, to_delete = self._update_observation(
                client, collection, observation, observationID, filenames)
        except Exception as e:
            if 'not found' in e._msg:
                if collection == 'CFHT':
                    try:
                        # retry on CFHTTERAPIX collection
                        observation = client.get_observation('CFHTTERAPIX', observationID)
                        updated, skipped, failed, to_delete = self._update_observation(
                            client, 'CFHTTERAPIX', observation, observationID, filenames)
                    except Exception as e:
                        if 'not found' in e._msg:
                            no_observation[observationID] = filenames
                        else:
                            failed[observationID] = filenames
                            self.logger.error(
                                'FAILED {} - Reason: {}'.format(observationID, e))
                else:
                    no_observation[observationID] = filenames
            else:
                failed[observationID] = filenames
                self.logger.error(
                    'FAILED {} - Reason: {}'.format(observationID, e))

        return no_observation, updated, skipped, failed, to_delete

    def _get_obs_from_file(self, obs_file, delimiter):
        junks = []
        obs = {}
        for l in obs_file:
            filenames = []
            try:
                obs_id, theRest = l.split(delimiter, 1)
                if obs_id is None or len(obs_id) == 0:
                    junks.append(l)
                    self.logger.debug('not a preview: {}'.format(l))
                else:
                    if obs_id in obs:
                        names = obs.get(obs_id)
                        names.append(l)
                        obs[obs_id] = names
                    else:
                        filenames.append(l)
                        obs[obs_id] = filenames
            except:
                junks.append(l)
                self.logger.debug('not a preview: {}'.format(l))

        with open(JUNK_REPORT, 'w') as the_file:
            for line in junks:
                the_file.write(line)
        return obs

    def _load_plugin_class(self, filepath):
        """
        Loads the plugin method and sets the self.plugin to refer to it.
        :param filepath: path to the file containing the python function
        """
        expected_class = 'ObservationUpdater'

        mod_name, file_ext = os.path.splitext(os.path.split(filepath)[-1])

        if file_ext.lower() == '.pyc':
            py_mod = imp.load_compiled(mod_name, filepath)
        else:
            py_mod = imp.load_source(mod_name, filepath)

        if hasattr(py_mod, expected_class):
            self.plugin = getattr(py_mod, expected_class)()
        else:
            raise Exception(
                'Cannot find ObservationUpdater class in pluging file ' +
                filepath)

        if not hasattr(self.plugin, 'update'):
            raise Exception(
                'Cannot find update method in plugin class ' + filepath)


def main_app():
    parser = util.get_base_parser(version=version.version,
                                  default_resource_id=DEFAULT_RESOURCE_ID)

    parser.description = (
        'An application which reads in a file containing names of files '
        'that are in AD but are not in CAOM and attempts to associate each '
        'named file with an observation. ')

    parser.add_argument("-s", "--server", help='URL of the CAOM2 repo server')

    parser.formatter_class = argparse.RawTextHelpFormatter

    subparsers = parser.add_subparsers(dest='cmd')

    # Note: RawTextHelpFormatter allows for the use of newline in epilog
    visit_parser = subparsers.add_parser(
        'visit', formatter_class=argparse.RawTextHelpFormatter,
        description='Visit observations in a collection',
        help='Visit observations in a collection')
    visit_parser.add_argument('--plugin', required=True,
                              type=argparse.FileType('r'),
                              help='plugin class to update each observation')
    visit_parser.add_argument(
        '--obs_file',
        help='file containing observations to be visited',
        type=argparse.FileType('r'))

    args = parser.parse_args()
    if len(sys.argv) < 2:
        parser.print_usage(file=sys.stderr)
        sys.stderr.write("{}: error: too few arguments\n".format(APP_NAME))
        sys.exit(-1)
    if args.verbose:
        level = logging.INFO
    elif args.debug:
        level = logging.DEBUG
    else:
        level = logging.WARN

    subject = net.Subject.from_cmd_line_args(args)
    server = None
    if args.server:
        server = args.server

    logging.basicConfig(
        format='%(asctime)s %(process)d %(levelname)-8s %(name)-12s ' +
               '%(funcName)s %(message)s',
        level=level, stream=sys.stdout)
    logger = logging.getLogger('main_app')
    client = CAOM2FixPreviewClient(subject, level, args.resource_id, host=server)
    if args.cmd == 'visit':
        try:
            client.visit(args.plugin.name, obs_file=args.obs_file)
        finally:
            if args.obs_file is not None:
                args.obs_file.close()
    else:
        logger.info("command {} not supported".format(args.cmd))

    logger.info("DONE")


if __name__ == '__main__':
    main_app()
