import { Module } from '@nestjs/common';
import { MdrService } from './mdr.service';
import { MdrCleanUpService } from './mdr.cleanup.service';
import { MdrReportService } from './mdr.report.service';
import { MdrIdentifyValideService } from './mdr.identify.valide.service';

@Module({
  exports: [
    MdrService, 
    MdrCleanUpService,
    MdrReportService
  ],
  providers: [
    MdrService, 
    MdrCleanUpService,
    MdrReportService,
    MdrIdentifyValideService
  ]
})
export class MdrModule {}
