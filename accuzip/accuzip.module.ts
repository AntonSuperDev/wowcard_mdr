import { Module } from '@nestjs/common';
import { BdayappendModule } from '../bdayappend/bdayappend.module';
import { AccuzipService } from './accuzip.service';
import { AccuzipWriteToDBService } from './accuzip.writetodb.service';
import { AccuzipOutputService } from './accuzip.output.service';
import { AccuzipCalculateDistanceService } from './accuzip.calculatedistance.service';

@Module({
  imports: [BdayappendModule],
  exports: [
    AccuzipWriteToDBService, 
    AccuzipService, 
    AccuzipOutputService
  ],
  providers: [
    AccuzipService, 
    AccuzipWriteToDBService, 
    AccuzipOutputService, 
    AccuzipCalculateDistanceService
  ]
})
export class AccuzipModule {}
